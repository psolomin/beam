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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

class ShardEventsHandler implements Subscriber<SubscribeToShardEventStream> {
  private static final Logger LOG = LoggerFactory.getLogger(ShardEventsHandler.class);
  private static final String LOG_MSG_TEMPLATE = "Stream = {} consumer = {} shard = {}";
  private static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 35_000;

  private final Long enqueueTimeoutMs;
  private final BlockingQueue<ShardEvent> queue;
  private final CountDownLatch eventsHandlerReadyLatch;
  private final String streamName;
  private final String consumerArn;
  private final String shardId;

  private Subscription s;
  private volatile boolean cancelled = false;

  ShardEventsHandler(
      Long enqueueTimeoutMs,
      BlockingQueue<ShardEvent> queue,
      CountDownLatch eventsHandlerReadyLatch,
      String streamName,
      String consumerArn,
      String shardId) {
    this.enqueueTimeoutMs = enqueueTimeoutMs;
    this.queue = queue;
    this.eventsHandlerReadyLatch = eventsHandlerReadyLatch;
    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.shardId = shardId;
  }

  ShardEventsHandler(
      BlockingQueue<ShardEvent> queue,
      CountDownLatch eventsHandlerReadyLatch,
      String streamName,
      String consumerArn,
      String shardId) {
    this(
        DEFAULT_ENQUEUE_TIMEOUT_MS,
        queue,
        eventsHandlerReadyLatch,
        streamName,
        consumerArn,
        shardId);
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    s = subscription;
    eventsHandlerReadyLatch.countDown();
  }

  /** AWS SDK Netty thread calls this at least every ~ 5 seconds even when no new records arrive. */
  @Override
  public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
    subscribeToShardEventStream.accept(
        new SubscribeToShardResponseHandler.Visitor() {
          @Override
          public void visit(SubscribeToShardEvent event) {
            enqueueEvent(ShardEvent.fromNext(event));
          }
        });
  }

  @Override
  public void onError(Throwable throwable) {
    if (queue.isEmpty()) {
      enqueueEvent(ShardEvent.error(throwable));
    }
    cancel();
  }

  /**
   * AWS SDK Netty thread calls this every ~ 5 minutes, these events alone are not enough signal to
   * conclude the shard has no more records to consume.
   */
  @Override
  public void onComplete() {
    LOG.info(LOG_MSG_TEMPLATE + " Complete", streamName, shardId, consumerArn);
    enqueueEvent(ShardEvent.subscriptionComplete());
  }

  void requestRecord() {
    if (!cancelled) {
      s.request(1);
    }
  }

  void cancel() {
    if (cancelled) {
      return;
    }
    cancelled = true;

    if (s != null) {
      s.cancel();
    }
  }

  private void enqueueEvent(ShardEvent event) {
    if (cancelled) {
      return;
    }

    try {
      if (!queue.offer(event, enqueueTimeoutMs, TimeUnit.MILLISECONDS)) {
        String template = LOG_MSG_TEMPLATE + " Queue wait time exceeded max {} ms";
        LOG.error(template, streamName, consumerArn, shardId, enqueueTimeoutMs);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
