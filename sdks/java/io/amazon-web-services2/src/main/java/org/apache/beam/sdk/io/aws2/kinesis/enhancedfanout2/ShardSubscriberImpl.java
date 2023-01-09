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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import java.util.List;
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
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.ConsumerError;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.RecoverableConsumerError;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.ShardEventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class ShardSubscriberImpl implements ShardSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscriberImpl.class);
  private final String shardId;
  private final Config config;
  private final BlockingQueue<ShardEventWrapper> shardEventsBuffer = new LinkedBlockingQueue<>(2);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final ShardSubscribersPool pool;
  private final ShardSubscribersPoolState state;
  private final RecordsBuffer recordsBuffer;
  private final AsyncClientProxy asyncClientProxy;

  ShardSubscriberImpl(
      Config config,
      String shardId,
      ClientBuilder clientBuilder,
      ShardSubscribersPool pool,
      ShardSubscribersPoolState initialState,
      RecordsBuffer recordsBuffer) {
    this.config = config;
    this.shardId = shardId;
    this.state = initialState;
    this.pool = pool;
    this.recordsBuffer = recordsBuffer;
    this.isRunning.set(true);
    this.asyncClientProxy = clientBuilder.build();
  }

  @Override
  public void run() {
    while (isRunning.get()) {
      try {
        ShardCheckpoint shardCheckpoint = state.getCheckpoint(shardId);
        boolean reSubscribe = subscribe(shardCheckpoint.toStartingPosition(), this::consume);
        if (!reSubscribe) break;
      } catch (InterruptedException e) {
        LOG.warn(
            "Interrupted in subscription loop for stream {} shard {}",
            config.getStreamName(),
            shardId);
      }
    }

    try {
      asyncClientProxy.close();
    } catch (Exception e) {
      LOG.warn("Failed while closing client: ", e);
    }
  }

  @Override
  public boolean stop() {
    // TODO: it takes too long to stop the subscription
    // but bufferPollTimeoutMs can't be < 5 seconds otherwise loop will re-subscribe all the time
    isRunning.set(false);
    return true;
  }

  boolean subscribe(
      StartingPosition startingPosition, Consumer<SubscribeToShardEvent> eventConsumer)
      throws InterruptedException {
    LOG.info("Creating subscription");
    KinesisShardEventsSubscriber shardEventsSubscriber = doSubscribe(startingPosition);
    return startConsumeLoop(eventConsumer, shardEventsSubscriber);
  }

  private KinesisShardEventsSubscriber doSubscribe(StartingPosition startingPosition)
      throws InterruptedException {
    UUID subscribeRequestId = UUID.randomUUID();
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(config.getConsumerArn())
            .shardId(shardId)
            .startingPosition(startingPosition)
            .build();

    LOG.info("Starting subscribe request {} - {}", subscribeRequestId, request);
    CountDownLatch eventsHandlerReadyLatch = new CountDownLatch(1);

    KinesisShardEventsSubscriber shardEventsSubscriber =
        new KinesisShardEventsSubscriber(
            shardEventsBuffer,
            eventsHandlerReadyLatch,
            config.getStreamName(),
            config.getConsumerArn(),
            shardId);

    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder()
            .onError(
                e ->
                    LOG.error(
                        "Failed to execute subscribe request {} - {}",
                        subscribeRequestId,
                        request,
                        e))
            .subscriber(() -> shardEventsSubscriber)
            .build();

    CompletableFuture<Void> f = asyncClientProxy.subscribeToShard(request, responseHandler);
    boolean subscriptionWasEstablished =
        eventsHandlerReadyLatch.await(config.getPoolStartTimeoutMs(), TimeUnit.MILLISECONDS);

    if (!subscriptionWasEstablished) {
      LOG.error("Subscribe request {} failed.", subscribeRequestId);
      if (!f.isCompletedExceptionally())
        LOG.warn(
            "subscribeToShard request {} failed, but future was not complete.", subscribeRequestId);
      // TODO: signal error to coordinator
      shardEventsSubscriber.cancel();
      throw new RuntimeException();
    }

    LOG.info("Subscription established.");
    shardEventsSubscriber.requestRecord();
    return shardEventsSubscriber;
  }

  private boolean startConsumeLoop(
      Consumer<SubscribeToShardEvent> eventConsumer,
      KinesisShardEventsSubscriber shardEventsSubscriber)
      throws InterruptedException {
    while (isRunning.get()) {
      ShardEventWrapper event =
          shardEventsBuffer.poll(
              config.getShardSubscriberBufferPollTimeoutMs(), TimeUnit.MILLISECONDS);

      if (event == null) {
        LOG.warn("No records available after {}", config.getShardSubscriberBufferPollTimeoutMs());
        break;
      }

      LOG.debug("Received event {}", event);
      switch (event.type()) {
        case SUBSCRIPTION_COMPLETE:
          {
            LOG.info("Shard {} - subscription complete", shardId);
            shardEventsSubscriber.cancel();
            return true;
          }
        case ERROR:
          {
            shardEventsSubscriber.cancel();
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
            shardEventsSubscriber.requestRecord();
            break;
          }
        case RE_SHARD:
          {
            handleReShard(event);
            shardEventsSubscriber.cancel();
            return false;
          }
        default:
          {
            LOG.warn("Unknown event type, ignoring: {}", event);
            break;
          }
      }
    }
    shardEventsSubscriber.cancel();
    return false;
  }

  void handleReShard(ShardEventWrapper event) {
    isRunning.set(false);
  }

  void handleCriticalError(ShardEventWrapper event) {
    pool.handleShardError(shardId, event);
  }

  private boolean maybeRecoverableError(ShardEventWrapper event) {
    Throwable error = event.getError();
    Throwable cause;
    if ((error instanceof CompletionException || error instanceof ExecutionException)
        && error.getCause() != null) {
      cause = ConsumerError.toConsumerError(error.getCause());
    } else {
      cause = ConsumerError.toConsumerError(error);
    }

    String msgTemplate = "Received error from shard handler. Stream {} Consumer {} Shard {}: {}";
    LOG.warn(
        msgTemplate,
        config.getStreamName(),
        config.getConsumerArn(),
        shardId,
        error.getClass().getName(),
        cause);
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

  private void consume(SubscribeToShardEvent event) {
    if (!event.records().isEmpty()) {
      List<KinesisClientRecord> clientRecords =
          new AggregatorUtil()
              .deaggregate(
                  event.records().stream()
                      .map(KinesisClientRecord::fromRecord)
                      .collect(Collectors.toList()));

      // TODO: check for return value when pushing
      clientRecords.forEach(
          r ->
              recordsBuffer.push(
                  Record.record(shardId, new KinesisRecord(r, config.getStreamName(), shardId))));
    } else {
      recordsBuffer.push(Record.checkPointOnly(shardId, event.continuationSequenceNumber()));
    }
  }
}
