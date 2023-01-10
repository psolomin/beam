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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;

class ShardSubscribersPoolStateImpl implements ShardSubscribersPoolState {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscribersPoolStateImpl.class);

  private final ConcurrentMap<String, ShardCheckpoint> shardsCheckpointsMap;

  ShardSubscribersPoolStateImpl(KinesisReaderCheckpoint initialCheckpoint) {
    ImmutableMap.Builder<String, ShardCheckpoint> b = ImmutableMap.builder();
    for (ShardCheckpoint shardCheckpoint : initialCheckpoint) {
      b.put(shardCheckpoint.getShardId(), shardCheckpoint);
    }
    this.shardsCheckpointsMap = new ConcurrentHashMap<>(b.build());
  }

  /**
   * This is called by Beam threads, for each records which is fetched from the main buffer.
   * Potential hot spot
   *
   * @param record to be ack-ed
   */
  @Override
  public void ackRecord(Record record) {
    // TODO: simplify this
    if (record.getKinesisRecord().isPresent()) {
      shardsCheckpointsMap.computeIfPresent(
          record.getShardId(),
          (k, v) ->
              v.moveAfter(record.getKinesisRecord().get(), record.getContinuationSequenceNumber()));
    } else {
      // checkpoint only
      // TODO: use optional instead
      if (record.getContinuationSequenceNumber() != null)
        shardsCheckpointsMap.computeIfPresent(
            record.getShardId(), (k, v) -> v.moveAfter(record.getContinuationSequenceNumber()));
      // ack-ing last record in the buffer, which is an artificial event without records
      else {
        ShardCheckpoint checkpointToDelete =
            Checkers.checkNotNull(
                shardsCheckpointsMap.get(record.getShardId()), record.getShardId());
        if (checkpointToDelete.shardIsClosed()) {
          LOG.info(
              "Ack-ed last record in shard {}. Removing it from checkpoint", record.getShardId());
          shardsCheckpointsMap.remove(record.getShardId());
        }
      }
    }
  }

  @Override
  public ShardCheckpoint getCheckpoint(String shardId) {
    return Checkers.checkNotNull(shardsCheckpointsMap.get(shardId), shardId);
  }

  /**
   * This called by pool coordinator thread, which means that the subscriber originating re-shard
   * event can not send new events to {@link RecordsBuffer}.
   *
   * <p>We still must wait for the last event to be consumed from {@link RecordsBuffer} and ack-ed,
   * before we can delete parentShardId data from the state.
   *
   * @param parentShardId shard which was closed
   * @param childShards carries data necessary for creating checkpoints for child shards
   */
  @Override
  public void applyReShard(
      String parentShardId, String lastSequenceNumber, List<ChildShard> childShards) {
    shardsCheckpointsMap.computeIfPresent(parentShardId, (k, v) -> v.markClosed());
  }

  /**
   * This is called by Beam threads, but it's assumed not to be called upon each record fetched from
   * the buffer -> fine not to pre-compute it and compute on-demand
   *
   * @return greatest timestamp among all ack-ed so far
   */
  @Override
  public Instant getWatermark() {
    return Instant.EPOCH;
  }

  /**
   * This is called by Beam threads, but it's assumed not to be called upon each record fetched from
   * the buffer -> fine not to pre-compute it and compute on-demand
   *
   * @return greatest timestamp among all ack-ed so far
   */
  @Override
  public KinesisReaderCheckpoint getCheckpointMark() {
    return new KinesisReaderCheckpoint(shardsCheckpointsMap.values());
  }
}
