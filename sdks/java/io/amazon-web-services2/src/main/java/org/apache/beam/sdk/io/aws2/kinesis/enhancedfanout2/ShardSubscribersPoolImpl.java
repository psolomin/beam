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
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.CriticalErrorSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.ReShardSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.ShardEventWrapper;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals.ShardSubscriberSignal;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class ShardSubscribersPoolImpl implements ShardSubscribersPool, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscribersPoolImpl.class);
  private static final String LOG_MSG_TEMPLATE = "Stream = {} consumer = {}";

  private static final long startTimeoutMs = 10_000;
  private static final long signalsOfferTimeoutMs = 1_000L;
  private static final long signalsPollTimeoutMs = 10_000L;
  private static final long awaitTerminationTimeoutMs = 30_000;
  private static final int signalsQueueCapacity = 1000;

  private final Config config;
  private final ClientBuilder clientBuilder;
  private final ExecutorService executorService;
  private final BlockingQueue<ShardSubscriberSignal> signals =
      new LinkedBlockingQueue<>(signalsQueueCapacity);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final CountDownLatch consumerStartedLatch;
  private final RecordsBuffer recordsBuffer;
  private final Map<String, ShardSubscriber> shardSubscribers = new ConcurrentHashMap<>();
  private final ShardSubscribersPoolState state;

  ShardSubscribersPoolImpl(
      Config config,
      ClientBuilder clientBuilder,
      ShardSubscribersPoolState initialState,
      RecordsBuffer recordsBuffer) {
    this.config = config;
    this.clientBuilder = clientBuilder;
    this.recordsBuffer = recordsBuffer;
    this.executorService = createThreadPool(config);
    this.state = initialState;
    this.consumerStartedLatch = new CountDownLatch(1);
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
      if (consumerStartedLatch.await(startTimeoutMs, TimeUnit.MILLISECONDS)) return true;
      else throw new RuntimeException(String.format("Did not start within %s ms", startTimeoutMs));
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
  public void handleShardError(String shardId, ShardEventWrapper event) {
    LOG.info("Error event from {}.", shardId, event.getError());
    try {
      CriticalErrorSignal signal = CriticalErrorSignal.fromError(shardId, event);
      if (!signals.offer(signal, signalsOfferTimeoutMs, TimeUnit.MILLISECONDS)) {
        LOG.warn(
            "Error event from {} was not pushed to the queue, timeout after {}ms",
            shardId,
            signalsOfferTimeoutMs);
      }
    } catch (InterruptedException e) {
      LOG.warn("Error event from {} was not pushed to the queue.", shardId, e);
    }
  }

  @Override
  public CustomOptional<KinesisRecord> nextRecord() {
    // TODO: handle this in nicer way
    CustomOptional<Record> maybeRecord = recordsBuffer.fetchOne();
    if (maybeRecord.isPresent() && maybeRecord.get().getKinesisRecord().isPresent())
      return CustomOptional.of(maybeRecord.get().getKinesisRecord().get());
    else return CustomOptional.absent();
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
        ShardSubscriberSignal signal = signals.poll(signalsPollTimeoutMs, TimeUnit.MILLISECONDS);
        if (signal != null) {
          if (signal instanceof ReShardSignal) processReShardSignal((ReShardSignal) signal);
          if (signal instanceof CriticalErrorSignal)
            processCriticalError((CriticalErrorSignal) signal);
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
    KinesisReaderCheckpoint initialCheckpoint = state.getCheckpointMark();
    initialCheckpoint
        .iterator()
        .forEachRemaining(
            shardCheckpoint -> {
              ShardSubscriber s =
                  new ShardSubscriberImpl(
                      config,
                      shardCheckpoint.getShardId(),
                      clientBuilder,
                      this,
                      state,
                      recordsBuffer);
              shardSubscribers.put(shardCheckpoint.getShardId(), s);
              executorService.submit(s);
            });
  }

  private void processReShardSignal(ReShardSignal reShardSignal) throws InterruptedException {
    LOG.info("Processing re-shard signal {}", reShardSignal);
  }

  private void processCriticalError(CriticalErrorSignal criticalErrorSignal) {
    LOG.error("Received unrecoverable error signal shard {}", criticalErrorSignal);
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
          executorService.awaitTermination(awaitTerminationTimeoutMs, TimeUnit.MILLISECONDS);
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
}
