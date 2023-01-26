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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class ShardSubscribersPoolImpl {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscribersPoolImpl.class);
  private static final int BUFFER_SIZE = 10_000;
  private static final int TARGET_MIN_REMAINING_CAPACITY = BUFFER_SIZE / 5;
  private static final long INITIAL_DEMAND_PER_SHARD = 2L;
  private static final long BUFFER_POLL_WAIT_MS = 1_000L;
  private static final long ENQUEUE_TIMEOUT_MS = 3_000;

  private final Config config;
  private final AsyncClientProxy kinesis;
  private final List<String> shardsIds;
  private final ConcurrentMap<String, KinesisShardEventsSubscriber> subscribers;
  private final ConcurrentMap<String, CompletableFuture<Void>> subscriptionFutures;
  private final BlockingQueue<ExtendedKinesisRecord> eventsBuffer;

  public ShardSubscribersPoolImpl(
      Config config, AsyncClientProxy kinesis, List<String> initialShardsIds) {
    this.config = config;
    this.kinesis = kinesis;
    this.shardsIds = initialShardsIds;
    this.subscribers = new ConcurrentHashMap<>();
    this.subscriptionFutures = new ConcurrentHashMap<>();
    this.eventsBuffer = new LinkedBlockingQueue<>(BUFFER_SIZE);
  }

  public boolean start() {
    // TODO: handle different types of start:
    // ShardIteratorType.LATEST
    // ShardIteratorType.AT_SEQUENCE_NUMBER
    // ShardIteratorType.TRIM_HORIZON
    // ShardIteratorType.AT_TIMESTAMP
    // AT_SEQUENCE_NUMBER:
    // - check if we have seen this seq number before
    // - check if we have seen a sub-sequence number before
    shardsIds.forEach(
        shardId ->
            subscriptionFutures.put(
                shardId,
                createSubscription(
                    shardId, StartingPosition.builder().type(ShardIteratorType.LATEST).build())));
    return true;
  }

  public boolean stop() {
    subscribers.values().forEach(KinesisShardEventsSubscriber::cancel);
    subscriptionFutures.values().forEach(f -> f.cancel(false));
    return true;
  }

  /**
   * This is ultimately called by Netty threads via {@link KinesisShardEventsSubscriber} callbacks.
   *
   * @param event
   * @throws InterruptedException
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void handleEvent(ShardEvent event) throws InterruptedException {
    switch (event.getType()) {
      case SUBSCRIPTION_COMPLETE:
        CompletableFuture.runAsync(
            () -> {
              // TODO: fetch continuation sequence number from checkpoint object
              subscriptionFutures.computeIfPresent(
                  event.getShardId(),
                  (k, v) ->
                      createSubscription(
                          k,
                          StartingPosition.builder()
                              // ShardIteratorType.AFTER_SEQUENCE_NUMBER
                              .type(ShardIteratorType.LATEST)
                              .build()));
            });
        break;
      case RE_SHARD:
        CompletableFuture.runAsync(
            () -> {
              List<String> successorShards = computeSuccessorShardsIds((ReShardEvent) event);
              successorShards.forEach(
                  s ->
                      subscriptionFutures.computeIfAbsent(
                          s,
                          k ->
                              createSubscription(
                                  k,
                                  StartingPosition.builder()
                                      .type(ShardIteratorType.TRIM_HORIZON)
                                      .build())));
            });
        break;
      case RECORDS:
        List<ExtendedKinesisRecord> records = ((RecordsShardEvent) event).getRecords();
        for (ExtendedKinesisRecord record : records) {
          if (!eventsBuffer.offer(record, ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            // We should never demand more than we consume.
            throw new RuntimeException("This should never happen");
          }
        }
        break;
      case ERROR:

      default:
        LOG.warn("Unknown event type {}", event.getType());
    }
  }

  private static List<String> computeSuccessorShardsIds(ReShardEvent event) {
    // FIXME: If future completes with unrecoverable error, entire reader should fail
    // FIXME: Down-sharding can cause shards to be "lost"! Re-visit this logic!
    LOG.info("Processing re-shard signal {}", event);

    List<String> successorShardsIds = new ArrayList<>();

    for (ChildShard childShard : event.getChildShards()) {
      if (childShard.parentShards().contains(event.getShardId())) {
        if (childShard.parentShards().size() > 1) {
          // This is the case of merging two shards into one.
          // when there are 2 parent shards, we only pick it up if
          // its max shard equals to sender shard ID
          String maxId = childShard.parentShards().stream().max(String::compareTo).get();
          if (event.getShardId().equals(maxId)) {
            successorShardsIds.add(childShard.shardId());
          }
        } else {
          // This is the case when shard is split
          successorShardsIds.add(childShard.shardId());
        }
      }
    }

    if (successorShardsIds.isEmpty()) {
      LOG.info("Found no successors for shard {}", event.getShardId());
    } else {
      LOG.info("Found successors for shard {}: {}", event.getShardId(), successorShardsIds);
    }
    return successorShardsIds;
  }

  private CompletableFuture<Void> createSubscription(
      String shardId, StartingPosition startingPosition) {
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
            this,
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

    CompletableFuture<Void> f = kinesis.subscribeToShard(request, responseHandler);
    boolean subscriptionWasEstablished = false;
    try {
      subscriptionWasEstablished =
          eventsHandlerReadyLatch.await(config.getPoolStartTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // handled below
    }

    if (!subscriptionWasEstablished) {
      LOG.error("Subscribe request {} failed.", subscribeRequestId);
      if (!f.isCompletedExceptionally()) {
        LOG.warn(
            "subscribeToShard request {} failed, but future was not complete.", subscribeRequestId);
      }
      shardEventsSubscriber.cancel();
      throw new RuntimeException();
    }

    subscribers.put(shardId, shardEventsSubscriber);
    shardEventsSubscriber.requestRecords(INITIAL_DEMAND_PER_SHARD);
    LOG.info("Subscription for shard {} established", shardId);
    return f;
  }

  public CustomOptional<KinesisRecord> nextRecord() {
    try {
      ExtendedKinesisRecord maybeRecord =
          eventsBuffer.poll(BUFFER_POLL_WAIT_MS, TimeUnit.MILLISECONDS);
      if (maybeRecord != null) {
        // ack record, e.g. mutate the checkpoint object
        if (eventsBuffer.remainingCapacity() > TARGET_MIN_REMAINING_CAPACITY) {
          KinesisShardEventsSubscriber shardSubscriber = subscribers.get(maybeRecord.getShardId());
          if (shardSubscriber != null) {
            shardSubscriber.requestRecords(1L);
          }
        }
        KinesisRecord record = maybeRecord.getKinesisRecord();
        if (record != null) {
          return CustomOptional.of(record);
        } else {
          return CustomOptional.absent();
        }
      } else return CustomOptional.absent();
    } catch (InterruptedException e) {
      return CustomOptional.absent();
    }
  }
}
