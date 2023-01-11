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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Checkers.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.CriticalErrorSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.ReShardSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.ShardEventWrapper;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.ShardSubscriberSignal;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;

public class ShardSubscribersPoolImpl implements ShardSubscribersPool, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscribersPoolImpl.class);
  private static final String LOG_MSG_TEMPLATE = "Stream = {} consumer = {}";

  private final Config config;
  private final AsyncClientProxy kinesis;
  private final ExecutorService executorService;
  private final BlockingQueue<ShardSubscriberSignal> signals;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final CountDownLatch consumerStartedLatch;
  private final RecordsBuffer recordsBuffer;
  private final Map<String, ShardSubscriber> shardSubscribers = new ConcurrentHashMap<>();
  private final ShardSubscribersPoolState state;

  ShardSubscribersPoolImpl(
      Config config,
      AsyncClientProxy kinesis,
      ShardSubscribersPoolState initialState,
      RecordsBuffer recordsBuffer) {
    this.config = config;
    this.kinesis = kinesis;
    this.recordsBuffer = recordsBuffer;
    this.executorService = createThreadPool(config);
    this.state = initialState;
    this.consumerStartedLatch = new CountDownLatch(1);
    this.signals = new LinkedBlockingQueue<>(config.getPoolSignalsQueueCapacity());
  }

  @Override
  public boolean start() {
    String coordinatorName =
        String.format(
            "shard-subscribers-pool-%s-%s", config.getStreamName(), config.getConsumerArn());
    Thread t = new Thread(this, coordinatorName);
    t.setDaemon(true);
    t.start();
    try {
      if (consumerStartedLatch.await(config.getPoolStartTimeoutMs(), TimeUnit.MILLISECONDS)) {
        return true;
      } else {
        throw new RuntimeException(
            String.format("Did not start within %s ms", config.getPoolStartTimeoutMs()));
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting to start");
    }
  }

  @Override
  public boolean stop() {
    initiateGracefulShutdown();
    return awaitTermination();
  }

  @Override
  public boolean isRunning() {
    return isRunning.get();
  }

  @Override
  public void sendReShardSignal(String shardId, ShardEventWrapper event) {
    LOG.info("Re-shard event from {}.", shardId);
    ReShardSignal signal = ReShardSignal.fromShardEvent(shardId, event);
    pushSignal(signal);
  }

  @Override
  public void sendShardErrorSignal(String shardId, ShardEventWrapper event) {
    LOG.warn("Error event from {}", shardId, event.getError());
    CriticalErrorSignal signal = CriticalErrorSignal.fromError(shardId, event);
    pushSignal(signal);
  }

  @Override
  public CustomOptional<KinesisRecord> nextRecord() {
    // TODO: handle this in nicer way
    CustomOptional<Record> maybeRecord = recordsBuffer.fetchOne();
    if (maybeRecord.isPresent() && maybeRecord.get().getKinesisRecord().isPresent()) {
      return CustomOptional.of(maybeRecord.get().getKinesisRecord().get());
    } else {
      return CustomOptional.absent();
    }
  }

  @Override
  public Instant getWatermark() {
    return state.getWatermark();
  }

  @Override
  public KinesisReaderCheckpoint getCheckpointMark() {
    return state.getCheckpointMark();
  }

  private static ExecutorService createThreadPool(Config config) {
    return Executors.newCachedThreadPool(
        new ThreadFactory() {
          private final AtomicLong threadCount = new AtomicLong(0);

          @Override
          public Thread newThread(@NonNull Runnable runnable) {
            String name =
                String.format(
                    "shard-consumer-%s-%s-%s",
                    threadCount.getAndIncrement(), config.getStreamName(), config.getConsumerArn());
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
          }
        });
  }

  @Override
  public void run() {
    isRunning.set(true);
    LOG.info(LOG_MSG_TEMPLATE + " Started", config.getStreamName(), config.getConsumerArn());
    createAndSubmitSubscribers();
    consumerStartedLatch.countDown();

    while (isRunning.get()) {
      try {
        ShardSubscriberSignal signal =
            signals.poll(config.getPoolSignalsPollTimeoutMs(), TimeUnit.MILLISECONDS);
        if (signal != null) {
          if (signal instanceof ReShardSignal) {
            processReShardSignal((ReShardSignal) signal);
          }
          if (signal instanceof CriticalErrorSignal) {
            processCriticalError((CriticalErrorSignal) signal);
          }
        } else {
          LOG.info("No re-shard events to handle");
        }
      } catch (InterruptedException e) {
        LOG.warn("Failed to take re-shard signal from queue. ", e);
      }
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void createAndSubmitSubscribers() {
    // FIXME: if future completes with unrecoverable error, entire reader should fail
    KinesisReaderCheckpoint initialCheckpoint = state.getCheckpointMark();
    initialCheckpoint
        .iterator()
        .forEachRemaining(
            shardCheckpoint -> {
              ShardSubscriber s =
                  new ShardSubscriberImpl(
                      config, shardCheckpoint.getShardId(), kinesis, this, state, recordsBuffer);
              shardSubscribers.put(shardCheckpoint.getShardId(), s);
              executorService.submit(s);
            });
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void processReShardSignal(ReShardSignal reShardSignal) throws InterruptedException {
    // FIXME: If future completes with unrecoverable error, entire reader should fail
    // FIXME: Down-sharding can cause shards to be "lost"! Re-visit this logic!
    LOG.info("Processing re-shard signal {}", reShardSignal);

    List<String> successorShardsIds = new ArrayList<>();

    for (ChildShard childShard : reShardSignal.getChildShards()) {
      if (childShard.parentShards().contains(reShardSignal.getSenderId())) {
        if (childShard.parentShards().size() > 1) {
          // This is the case of merging two shards into one.
          // when there are 2 parent shards, we only pick it up if
          // its max shard equals to sender shard ID
          String maxId = childShard.parentShards().stream().max(String::compareTo).get();
          if (reShardSignal.getSenderId().equals(maxId)) {
            successorShardsIds.add(childShard.shardId());
          }
        } else {
          // This is the case when shard is split
          successorShardsIds.add(childShard.shardId());
        }
      }
    }

    checkNotNull(shardSubscribers.get(reShardSignal.getSenderId()), reShardSignal.getSenderId())
        .stop();
    shardSubscribers.remove(reShardSignal.getSenderId());

    if (successorShardsIds.isEmpty()) {
      LOG.info("Found no successors for shard {}", reShardSignal.getSenderId());
      // at this point, current split pool can end up without shards at all.
      if (shardSubscribers.size() == 0) {
        LOG.info("Pool doesn't have shards after processing {}. Shutting down.", reShardSignal);
        initiateGracefulShutdown();
        return;
      }
    } else {
      LOG.info(
          "Found successors for shard {}: {}", reShardSignal.getSenderId(), successorShardsIds);
    }
    state.applyReShard(reShardSignal.getSenderId(), successorShardsIds);
    reShardSignal
        .getChildShards()
        .forEach(
            childShard -> {
              if (!shardSubscribers.containsKey(childShard.shardId())) {
                ShardSubscriber s =
                    new ShardSubscriberImpl(
                        config, childShard.shardId(), kinesis, this, state, recordsBuffer);
                shardSubscribers.put(childShard.shardId(), s);
                executorService.submit(s);
              }
            });
  }

  private void processCriticalError(CriticalErrorSignal criticalErrorSignal) {
    LOG.error("Processing unrecoverable error signal shard {}", criticalErrorSignal);
    initiateGracefulShutdown();
  }

  private void initiateGracefulShutdown() {
    LOG.info(
        LOG_MSG_TEMPLATE + " Initiating shutdown", config.getStreamName(), config.getConsumerArn());
    isRunning.set(false);
    try {
      try {
        cleanUpResources();
      } catch (Exception e) {
        LOG.warn(
            LOG_MSG_TEMPLATE + " Error while cleaning up.",
            config.getStreamName(),
            config.getConsumerArn(),
            e);
      }
    } finally {
      executorService.shutdown();
    }

    LOG.info(
        LOG_MSG_TEMPLATE + " Shutdown complete", config.getStreamName(), config.getConsumerArn());
  }

  private boolean awaitTermination() {
    try {
      boolean isTerminated =
          executorService.awaitTermination(
              config.getPoolAwaitTerminationTimeoutMs(), TimeUnit.MILLISECONDS);
      if (!isTerminated) {
        LOG.warn("Unable to gracefully shut down");
      }
      return isTerminated;
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while shutting down");
    }
    return false;
  }

  void cleanUpResources() {
    shardSubscribers.forEach((k, v) -> v.stop());
  }

  private void pushSignal(ShardSubscriberSignal signal) {
    try {
      if (!signals.offer(signal, config.getPoolSignalsOfferTimeoutMs(), TimeUnit.MILLISECONDS)) {
        LOG.warn(
            "Error event from {} was not pushed to the queue, timeout after {}ms",
            signal,
            config.getPoolSignalsOfferTimeoutMs());
      }
    } catch (InterruptedException e) {
      LOG.warn("Error event from {} was not pushed to the queue.", signal.getSenderId(), e);
    }
  }
}
