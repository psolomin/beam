/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.errors.ConsumerError;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.errors.RecoverableConsumerError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

class ShardSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscriber.class);
  private static final long START_TIMEOUT_MS = 10_000;
  private static final int STOP_ATTEMPTS_MAX = 4;
  private static final long STOP_TIMEOUT = 1000L / STOP_ATTEMPTS_MAX;
  private static final int QUEUE_CAPACITY = 2;
  private static final long DEFAULT_QUEUE_POLL_TIMEOUT_MS = 15_000;
  private final StreamConsumer pool;
  private final BlockingQueue<ShardEvent> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
  private final KinesisAsyncClientProxy client;
  private final String streamName;
  private final String consumerArn;
  private final String shardId;
  private final AtomicBoolean isActive;
  private CustomOptional<ShardEventsHandler> shardEventsHandler = CustomOptional.absent();

  ShardSubscriber(
      StreamConsumer pool,
      KinesisAsyncClientProxy client,
      String streamName,
      String consumerArn,
      String shardId) {
    this.pool = pool;
    this.client = client;
    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.shardId = shardId;
    this.isActive = new AtomicBoolean(false);
  }

  boolean subscribe(
      StartingPosition startingPosition, Consumer<SubscribeToShardEvent> eventConsumer)
      throws InterruptedException {
    LOG.info("Creating subscription");
    shardEventsHandler = CustomOptional.of(doSubscribe(startingPosition));
    isActive.set(true);
    return startConsumeLoop(eventConsumer, shardEventsHandler.get());
  }

  private ShardEventsHandler doSubscribe(StartingPosition startingPosition)
      throws InterruptedException {
    UUID subscribeRequestId = UUID.randomUUID();
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(consumerArn)
            .shardId(shardId)
            .startingPosition(startingPosition)
            .build();

    LOG.info("Starting subscribe request {} - {}", subscribeRequestId, request);
    CountDownLatch eventsHandlerReadyLatch = new CountDownLatch(1);

    ShardEventsHandler subscriber =
        new ShardEventsHandler(queue, eventsHandlerReadyLatch, streamName, consumerArn, shardId);

    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder()
            .onError(
                e ->
                    LOG.error(
                        "Failed to execute subscribe request {} - {}",
                        subscribeRequestId,
                        request,
                        e))
            .subscriber(() -> subscriber)
            .build();

    CompletableFuture<Void> f = client.subscribeToShard(request, responseHandler);
    boolean subscriptionWasEstablished =
        eventsHandlerReadyLatch.await(START_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    if (!subscriptionWasEstablished) {
      LOG.error("Subscribe request {} failed.", subscribeRequestId);
      if (!f.isCompletedExceptionally())
        LOG.warn(
            "subscribeToShard request {} failed, but future was not complete.", subscribeRequestId);
      // TODO: signal error to coordinator
      subscriber.cancel();
      throw new RuntimeException();
    }

    LOG.info("Subscription established.");
    subscriber.requestRecord();
    return subscriber;
  }

  private boolean startConsumeLoop(
      Consumer<SubscribeToShardEvent> eventConsumer, ShardEventsHandler shardEventsHandler)
      throws InterruptedException {
    LOG.debug("Starting consumer");

    while (isActive.get()) {
      ShardEvent event = queue.poll(DEFAULT_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      LOG.debug("Received event {}", event);

      if (event == null) {
        LOG.info("No records available after {}", DEFAULT_QUEUE_POLL_TIMEOUT_MS);
        break;
      }

      switch (event.type()) {
        case SUBSCRIPTION_COMPLETE:
          {
            LOG.info("Shard {} - subscription complete", shardId);
            return true;
          }
        case ERROR:
          {
            if (maybeRecoverableError(event)) return true;
            else {
              handleCriticalError(event);
              return false;
            }
          }
        case RECORDS: // TODO: this may throw
          {
            SubscribeToShardEvent subscribeToShardEvent = event.getWrappedEvent();
            eventConsumer.accept(subscribeToShardEvent);
            shardEventsHandler.requestRecord();
            break;
          }
        case RE_SHARD:
          {
            handleReShard(event);
            return false;
          }
        default:
          {
            LOG.warn("Unknown event type, ignoring: {}", event);
            break;
          }
      }
    }

    return false;
  }

  void handleReShard(ShardEvent event) {
    pool.handleReShard(this.shardId, event);
    isActive.set(false);
  }

  void handleCriticalError(ShardEvent event) {
    pool.handleShardError(this.shardId, event);
  }

  private boolean maybeRecoverableError(ShardEvent event) {
    Throwable error = event.getError();
    Throwable cause;
    if ((error instanceof CompletionException || error instanceof ExecutionException)
        && error.getCause() != null) {
      cause = ConsumerError.toConsumerError(error.getCause());
    } else {
      cause = ConsumerError.toConsumerError(error);
    }

    String msgTemplate = "Received error from shard handler. Stream {} Consumer {} Shard {}: {}";
    LOG.warn(msgTemplate, streamName, consumerArn, shardId, error.getClass().getName(), cause);
    return isRecoverable(cause);
  }

  private boolean isRecoverable(Throwable cause) {
    if (cause instanceof RecoverableConsumerError) {
      LOG.warn("Netty thread was not able to submit outstanding record.");
      return true;
    } else {
      return false;
    }
  }

  void cancel() {
    if (isActive.get()) shardEventsHandler.get().cancel();

    int attemptNo = 1;
    while (!queue.isEmpty() && attemptNo <= STOP_ATTEMPTS_MAX) {
      LOG.warn("Queue is not empty! Waiting {}ms to consume outstanding records.", STOP_TIMEOUT);
      try {
        synchronized (queue) {
          queue.wait(STOP_TIMEOUT);
        }
      } catch (InterruptedException e) {
        LOG.warn("Queue is not empty! Data loss");
      }
      attemptNo++;
    }
  }
}
