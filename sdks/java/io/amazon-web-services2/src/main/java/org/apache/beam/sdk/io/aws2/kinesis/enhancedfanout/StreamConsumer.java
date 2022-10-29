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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.ShardsListingUtils.getShardsAfterParent;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicy;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicyFactory;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.CriticalErrorSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.ReShardSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals.ShardSubscriberSignal;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.Record;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.RecordsSink;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

public class StreamConsumer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);
  private static final String LOG_MSG_TEMPLATE = "Stream = {} consumer = {}";

  private static final long startTimeoutMs = 10_000;
  private static final long signalsOfferTimeoutMs = 1_000L;
  private static final Long signalsPollTimeoutMs = 10_000L;
  private static final long awaitTerminationTimeoutMs = 30_000;

  private final Config config;
  private final ClientBuilder clientBuilder;
  private final ExecutorService executorService;
  private final Map<String, ShardEventsConsumer> consumers = new ConcurrentHashMap<>();
  private final BlockingQueue<ShardSubscriberSignal> signals =
      new LinkedBlockingQueue<>(Integer.MAX_VALUE);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final RecordsSink recordsSink;
  private final CountDownLatch consumerStartedLatch;
  private final KinesisReaderCheckpoint initialCheckpoint;

  private StreamConsumer(
      Config config,
      ClientBuilder clientBuilder,
      KinesisReaderCheckpoint initialCheckpoint,
      RecordsSink recordsSink,
      CountDownLatch consumerStartedLatch) {
    LOG.info(LOG_MSG_TEMPLATE + " Creating pool", config.getStreamName(), config.getConsumerArn());

    this.config = config;
    this.clientBuilder = clientBuilder;
    this.executorService = createThreadPool(config);
    this.recordsSink = recordsSink;
    this.consumerStartedLatch = consumerStartedLatch;
    this.initialCheckpoint = initialCheckpoint;
  }

  public static StreamConsumer init(
      Config config,
      ClientBuilder clientBuilder,
      KinesisReaderCheckpoint initialCheckpoint,
      RecordsSink recordsSink) {
    String coordinatorName =
        String.format(
            "shard-consumers-pool-coordinator-%s-%s",
            config.getStreamName(), config.getConsumerArn());
    CountDownLatch latch = new CountDownLatch(1);
    StreamConsumer streamConsumer =
        new StreamConsumer(config, clientBuilder, initialCheckpoint, recordsSink, latch);
    Thread t = new Thread(streamConsumer, coordinatorName);
    t.setDaemon(true);
    t.start();
    try {
      if (latch.await(startTimeoutMs, TimeUnit.MILLISECONDS)) return streamConsumer;
      else throw new RuntimeException(String.format("Did not start within %s ms", startTimeoutMs));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting to start");
    }
  }

  public void initiateGracefulShutdown() {
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

  public void awaitTermination() throws InterruptedException {
    if (!executorService.awaitTermination(awaitTerminationTimeoutMs, TimeUnit.MILLISECONDS)) {
      LOG.warn("Unable to gracefully shut down");
    }
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  private void cleanUpResources() {
    consumers.forEach((k, v) -> v.initiateGracefulShutdown());
  }

  private ExecutorService createThreadPool(Config config) {
    return Executors.newCachedThreadPool(
        new ThreadFactory() {
          private final AtomicLong threadCount = new AtomicLong(0);

          @Override
          public Thread newThread(Runnable runnable) {
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

  void handleReShard(String shardId, ShardEvent event) {
    SubscribeToShardEvent wrappedEvent = event.getWrappedEvent();
    LOG.info("Re-shard event from {}. New shards: {}.", shardId, wrappedEvent.childShards());
    try {
      if (!signals.offer(
          ReShardSignal.fromShardEvent(shardId, event),
          signalsOfferTimeoutMs,
          TimeUnit.MILLISECONDS)) {
        LOG.warn(
            "Re-shard event from {} was not pushed to the queue, timeout after {}ms",
            shardId,
            signalsOfferTimeoutMs);
      }
    } catch (InterruptedException e) {
      LOG.warn("Re-shard event from {} was not pushed to the queue", shardId, e);
    }
  }

  void handleShardError(String shardId, ShardEvent event) {
    LOG.info("Error event from {}.", shardId, event.getError());
    try {
      if (!signals.offer(
          CriticalErrorSignal.fromError(shardId, event),
          signalsOfferTimeoutMs,
          TimeUnit.MILLISECONDS)) {
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
  public void run() {
    submitShardConsumersTasks(initialCheckpoint);
    isRunning.set(true);
    LOG.info(LOG_MSG_TEMPLATE + " Started", config.getStreamName(), config.getConsumerArn());
    consumerStartedLatch.countDown();

    while (isRunning.get()) {
      try {
        ShardSubscriberSignal signal = signals.poll(signalsPollTimeoutMs, TimeUnit.MILLISECONDS);
        if (signal != null) {
          if (signal instanceof ReShardSignal) processReShardSignal((ReShardSignal) signal);
          if (signal instanceof CriticalErrorSignal)
            processCriticalError((CriticalErrorSignal) signal);
        } else {
          LOG.info("No re-shard events to handle. Consumers cnt = {}", consumers.size());
        }
      } catch (InterruptedException e) {
        LOG.warn("Failed to take re-shard signal from queue. ", e);
      }
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void submitShardConsumersTasks(KinesisReaderCheckpoint initialCheckpoint) {
    ImmutableMap.Builder<String, ShardEventsConsumer> shardsConsumers = ImmutableMap.builder();
    for (ShardCheckpoint checkpoint : initialCheckpoint) {
      ShardEventsConsumerState state = initState(checkpoint.getShardId());
      ShardEventsConsumer consumer =
          ShardEventsConsumer.fromShardEventsConsumerState(
              this, config, clientBuilder, recordsSink, state);
      shardsConsumers.put(checkpoint.getShardId(), consumer);
    }

    consumers.putAll(shardsConsumers.build());
    consumers.values().forEach(executorService::submit);
  }

  private ShardEventsConsumerState initState(String shardId, String continuationSequenceNumber) {
    ShardCheckpoint checkpoint =
        new ShardCheckpoint(
            config.getStreamName(),
            config.getConsumerArn(),
            shardId,
            ShardIteratorType.AFTER_SEQUENCE_NUMBER,
            continuationSequenceNumber,
            null);

    WatermarkPolicy watermarkPolicy =
        WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();
    return new ShardEventsConsumerStateImpl(
        config.getStreamName(),
        shardId,
        checkpoint,
        watermarkPolicy,
        WatermarkPolicyFactory.withArrivalTimePolicy());
  }

  private ShardEventsConsumerState initState(String shardId) {
    ShardCheckpoint checkpoint =
        new ShardCheckpoint(
            config.getStreamName(), config.getConsumerArn(), shardId, config.getStartingPoint());

    WatermarkPolicy watermarkPolicy =
        WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

    return new ShardEventsConsumerStateImpl(
        config.getStreamName(),
        shardId,
        checkpoint,
        watermarkPolicy,
        WatermarkPolicyFactory.withArrivalTimePolicy());
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void processReShardSignal(ReShardSignal reShardSignal) throws InterruptedException {
    LOG.info("Processing re-shard signal {}", reShardSignal);
    String receivedFromShardId = reShardSignal.getSenderId();
    List<ChildShard> childShards = reShardSignal.getChildShards();
    List<Shard> newShards = waitForNewShards(receivedFromShardId, childShards);

    LOG.info("Stopping {} upon signal {}", receivedFromShardId, reShardSignal);
    consumers.get(receivedFromShardId).initiateGracefulShutdown();

    newShards.forEach(
        newShard -> {
          String id = newShard.shardId();
          if (!consumers.containsKey(id)) {
            LOG.info("Starting {} upon signal {}", id, reShardSignal);
            String beginningSeqNumber = newShard.sequenceNumberRange().startingSequenceNumber();
            ShardEventsConsumerState state = initState(id, beginningSeqNumber);
            ShardEventsConsumer newConsumer =
                ShardEventsConsumer.fromShardEventsConsumerState(
                    this, config, clientBuilder, recordsSink, state);
            consumers.put(id, newConsumer);
            executorService.submit(newConsumer);
          }
        });
  }

  private void processCriticalError(CriticalErrorSignal criticalErrorSignal) {
    LOG.error(
        "Received unrecoverable error from shard {}",
        criticalErrorSignal.getSenderId(),
        criticalErrorSignal.getError());
    initiateGracefulShutdown();
  }

  private List<Shard> waitForNewShards(String receivedFromShardId, List<ChildShard> expectedShards)
      throws InterruptedException {
    int maxAttempts = 3;
    int waitBetweenAttemptsMs = 1_000;
    int currentAttempt = 1;

    Set<String> expectedShardsIds =
        expectedShards.stream().map(ChildShard::shardId).collect(Collectors.toSet());
    while (currentAttempt <= maxAttempts) {
      List<Shard> newShards =
          getShardsAfterParent(receivedFromShardId, config, clientBuilder).stream()
              .filter(s -> expectedShardsIds.contains(s.shardId()))
              .collect(Collectors.toList());

      Set<String> newShardsIds = newShards.stream().map(Shard::shardId).collect(Collectors.toSet());
      expectedShardsIds.removeAll(newShardsIds);
      if (expectedShardsIds.size() == 0) return newShards;
      else {
        Thread.sleep(waitBetweenAttemptsMs);
      }
      currentAttempt++;
    }

    String msg =
        String.format(
            "After %s attempts still not found shard info for %s",
            currentAttempt, expectedShardsIds);
    throw new RuntimeException(msg);
  }

  public CustomOptional<KinesisRecord> nextRecord() {
    Record record = recordsSink.fetch();
    if (record == null) return CustomOptional.absent();

    consumers.get(record.getShardId()).ackRecord(record);
    if (record.getKinesisClientRecord().isPresent()) {
      KinesisRecord kinesisRecord =
          new KinesisRecord(
              record.getKinesisClientRecord().get(), config.getStreamName(), record.getShardId());
      return CustomOptional.of(kinesisRecord);
    } else {
      return CustomOptional.absent();
    }
  }

  public Instant getWatermark() {
    return getMinTimestamp(ShardEventsConsumer::getShardWatermark);
  }

  private Instant getMinTimestamp(Function<ShardEventsConsumer, Instant> timestampExtractor) {
    return consumers.values().stream()
        .map(timestampExtractor)
        .min(Comparator.naturalOrder())
        .orElse(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  public KinesisReaderCheckpoint getCheckpointMark() {
    ImmutableMap<String, ShardEventsConsumer> currentConsumers = ImmutableMap.copyOf(consumers);
    return new KinesisReaderCheckpoint(
        currentConsumers.values().stream()
            .map(ShardEventsConsumer::getCheckpoint)
            .collect(Collectors.toList()));
  }
}
