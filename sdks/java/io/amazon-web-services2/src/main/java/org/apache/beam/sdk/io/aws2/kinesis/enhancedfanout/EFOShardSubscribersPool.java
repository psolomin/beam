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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ForwardingIterator;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

/**
 * TODO: - switch to existing ShardCheckpoint - we don't care what consumer name was, we want to
 * switch back and forth from / to EFO - back-fill - EFO consumer, then use normal consumer to keep
 * going - test with real Kinesis - check whenComplete (netty) vs whenCompleteAsync (fork join pool)
 * - Work on testing & stub - think of delay of re-connect (in the pool -
 * oneThreadScheduledExecService) - Re-sharding: state is always reflecting the ack-ed checkpoint,
 * so getNextRecord() executes actual state mutations. The caller of that thing will handle starting
 * new subscriptions and deleting the orphaned shards' checkpoints from the map. - Inner classes are
 * fine: - You limit public surface, clear isolation - Everything in the package level - hard to
 * navigate, too many classes - Inner class can be used only in the context of enclosing class, it
 * is more clear - Readers don't need to worry about all the places class can be re-used -
 * KinesisIO.Read / Write - Javadoc is easier to navigate - Sometimes classes with inner classes
 * become too huge - Static helper classes (Util) used in a single place - also good candidates for
 * inner classes - If you really re-use over and over - better not to - DirectRunner interrupts and
 * re-creates new sources too often. Use FlinkRunner - Config class - potential alternative - Client
 * configuration - option: not to have back-offs in custom code, client itself has back-offs
 * internally - we can think of this later, add any custom back offs as late as possible - ?
 * recommend in javadoc to use large initial back offs ?
 */
@SuppressWarnings({"nullness"})
class EFOShardSubscribersPool {
  private static final Logger LOG = LoggerFactory.getLogger(EFOShardSubscribersPool.class);

  // TODO: get rid of these 2?
  private final UUID poolId;
  private final Config config;

  private final KinesisIO.Read read;
  private final KinesisAsyncClient kinesis;

  /**
   * Unbounded queue of events, but events in-flight are limited by the {@link EFOShardSubscriber}.
   */
  private final ConcurrentLinkedQueue<EventRecords> eventQueue = new ConcurrentLinkedQueue<>();

  /**
   * State map of currently active shards that can be checkpointed.
   *
   * <p>This map may only be updated from within {@link #getNextRecord()} (and dependent {@link
   * #onEventDone}).
   */
  private final Map<String, ShardState> state = new ConcurrentHashMap<>();

  /**
   * Async subscription error (as first seen), if set all subscribers must be cancelled and no new
   * ones started.
   *
   * <p>Must be volatile as it is accessed from various threads. But it's best effort, setting this
   * doesn't have to be atomic.
   */
  volatile @MonotonicNonNull Throwable subscriptionError;

  /**
   * Async completion callback handling {@link EFOShardSubscriber#subscribe supscriptions} that
   * terminate exceptionally.
   *
   * <p>Unless already in error state, stores error as {@link #subscriptionError} and cancels all
   * subscribers in {@link #state} to drain the {@link #eventQueue}. The {@link #subscriptionError}
   * is only propagated when the queue is empty as this simplifies state management and
   * checkpointing a lot.
   */
  private final BiConsumer<Void, Throwable> errorHandler =
      (Void unused, Throwable error) -> {
        if (error != null && subscriptionError == null) {
          subscriptionError = error;
          state.forEach((k, v) -> v.subscriber.cancel());
        }
      };

  // EventRecords iterator that is currently consumed
  @Nullable EventRecords current = null;

  EFOShardSubscribersPool(Config config, KinesisIO.Read readSpec, KinesisAsyncClient kinesis) {
    this.poolId = UUID.randomUUID();
    this.config = config;
    this.read = readSpec;
    this.kinesis = kinesis;
  }

  /**
   * Starts a subscribers pool by starting a {@link EFOShardSubscriber#subscribe shard subscription}
   * for each {@link ShardCheckpoint} with the subscription {@link #errorHandler} callback.
   *
   * <p>{@link EFOShardSubscriber}s with their respective state are tracked in {@link #state}.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start(Iterable<ShardCheckpoint> checkpoints) {
    List<String> startingWithShards = checkpointToShardsIds(checkpoints);
    LOG.info(
        "Starting pool {} {} {}. Shards = {}",
        poolId,
        config.getStreamName(),
        config.getConsumerArn(),
        startingWithShards);
    checkpoints.forEach(
        ch -> {
          EFOShardSubscriber subscriber =
              new EFOShardSubscriber(this, ch.getShardId(), read, kinesis);
          StartingPosition startingPosition = StartingPosition.builder()
                          .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                  .sequenceNumber(ch.getSequenceNumber())
                                          .build();
          subscriber.subscribe(startingPosition).whenCompleteAsync(errorHandler);
          ShardState shardState = new ShardState(subscriber);
          state.putIfAbsent(ch.getShardId(), shardState);
        });
  }

  /**
   * FIXME: this has some bug
   *
   * Returns the next deaggregated {@link KinesisRecord} if available and updates {@link #state}
   * accordingly so that it reflects a mutable checkpoint AFTER returning that record.
   *
   * <p>Async subscription errors are delayed until {@link #eventQueue} is completely drained and
   * then rethrown here.
   *
   * <p>This repeats the following steps until a record or {@code null} was returned:
   *
   * <ol>
   *   <li>If {@link #current} is null and {@link #eventQueue} is empty, return {@code null} unless
   *       {@link #subscriptionError} is set: in that case rethrow.
   *   <li>Otherwise if {@link #current} is null, poll next from {@link #eventQueue}.
   *   <li>If {@link #current} has a next {@link KinesisClientRecord}, update {@link #state}
   *       accordingly and return the corresponding converted {@link KinesisRecord}, optionally
   *       triggering {@link #onEventDone} if that was the last record of {@link #current}.
   *   <li>Finally, if nothing was returned yet, trigger {@link #onEventDone} and continue loop.
   * </ol>
   */
  @Nullable
  KinesisRecord getNextRecord() throws IOException {
    while (true) {
      if (current == null && eventQueue.isEmpty()) {
        if (subscriptionError == null) {
          return null;
        } else {
          throw new IOException(subscriptionError);
        }
      }

      current = eventQueue.poll();
      if (current != null) {
        String shardId = current.shardId;
        ShardState shardState = Preconditions.checkStateNotNull(state.get(shardId));
        if (current != null && current.hasNext()) {
          KinesisClientRecord r = current.next();
          if (!current.hasNext()) {
            onEventDone(shardState, current);
            current = null;
          }
          shardState.update(r);
          return new KinesisRecord(r, config.getStreamName(), shardId);
        } else {
          onEventDone(shardState, checkArgumentNotNull(current));
          shardState.update(current);
          current = null;
        }
      }
    }
  }

  /**
   * Unsets {@link #current} and updates {@link #state} accordingly.
   *
   * <p>If {@link SubscribeToShardEvent#continuationSequenceNumber()} is defined, update {@link
   * ShardState} accordingly. Otherwise, or if {@link SubscribeToShardEvent#childShards()} exists,
   * handle re-sharding: remove old shard from {@link #state} and add new ones at TRIM_HORIZON.
   *
   * <p>In case of re-sharding, start all new {@link EFOShardSubscriber#subscribe subscriptions}
   * with the subscription {@link #errorHandler} if there is no {@link #subscriptionError} yet.
   *
   * <p>Finally, {@link EFOShardSubscriber#ackEvent() acknowledge} to the respective {@link
   * EFOShardSubscriber} that processing of an event is complete.
   */
  private void onEventDone(ShardState shardState, EventRecords records) {
    if (records.event.hasChildShards()) {
      LOG.info("Child shards: {} ", records.event.childShards());
    }
    shardState.subscriber.ackEvent();
  }

  /** Adds a {@link EventRecords} iterator for shardId and event to {@link #eventQueue}. */
  void enqueueEvent(String shardId, SubscribeToShardEvent event) {
    eventQueue.offer(new EventRecords(config.getStreamName(), shardId, event));
  }

  public Instant getWatermark() {
    return Instant.EPOCH;
  }

  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return new KinesisReaderCheckpoint(Collections.EMPTY_LIST);
  }

  public boolean stop() {
    return true;
  }

  /**
   * Mutable class tracking state and progress per shard.
   *
   * <p>A {@link ShardCheckpoint} is the immutable correspondence to this using iterator type {@link
   * ShardIteratorType#AFTER_SEQUENCE_NUMBER} or {@link ShardIteratorType#TRIM_HORIZON} for new
   * shards if {@link #sequenceNumber} is not set yet.
   */
  private static class ShardState {
    EFOShardSubscriber subscriber;
    @Nullable String sequenceNumber = null;
    long subSequenceNumber = 0L;

    ShardState(EFOShardSubscriber subscriber) {
      this.subscriber = subscriber;
    }

    ShardState update(KinesisClientRecord r) {
      sequenceNumber = r.sequenceNumber();
      subSequenceNumber = r.subSequenceNumber();
      return this;
    }

    ShardState update(EventRecords eventRecords) {
      sequenceNumber = eventRecords.event.continuationSequenceNumber();
      subSequenceNumber = 0L;
      return this;
    }
  }

  /**
   * Lazy iterator over deaggregated {@link KinesisClientRecord}s of {@link #event}.
   *
   * <p>Event {@link Record}s are lazily deaggregated using {@link AggregatorUtil} when {@link
   * ForwardingIterator#delegate()} is first called.
   */
  private static class EventRecords extends ForwardingIterator<KinesisClientRecord> {
    String streamName;
    String shardId;
    SubscribeToShardEvent event;
    @MonotonicNonNull Iterator<KinesisClientRecord> delegate = null;

    public EventRecords(String streamName, String shardId, SubscribeToShardEvent event) {
      this.streamName = streamName;
      this.shardId = shardId;
      this.event = event;
    }

    @Override
    protected Iterator<KinesisClientRecord> delegate() {
      if (event.hasRecords() && !event.records().isEmpty()) {
        if (delegate == null) {
          AggregatorUtil au = new AggregatorUtil();
          delegate =
              au.deaggregate(
                      event.records().stream()
                          .map(KinesisClientRecord::fromRecord)
                          .collect(Collectors.toList()))
                  .iterator();
        }
      } else {
        delegate = Collections.emptyIterator();
      }
      return delegate;
    }
  }

  private List<String> checkpointToShardsIds(Iterable<ShardCheckpoint> checkpoints) {
    List<String> result = new ArrayList<>();
    checkpoints.forEach(chk -> result.add(chk.getShardId()));
    return Collections.unmodifiableList(result);
  }

  UUID getPoolId() {
    return poolId;
  }
}
