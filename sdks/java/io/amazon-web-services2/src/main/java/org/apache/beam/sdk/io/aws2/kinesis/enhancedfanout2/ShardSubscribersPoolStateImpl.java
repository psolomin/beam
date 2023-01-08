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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;

class ShardSubscribersPoolStateImpl implements ShardSubscribersPoolState {
  private final ConcurrentMap<String, ShardCheckpoint> shardsCheckpointsMap;

  ShardSubscribersPoolStateImpl(KinesisReaderCheckpoint initialCheckpoint) {
    ImmutableMap.Builder<String, ShardCheckpoint> b = ImmutableMap.builder();
    for (ShardCheckpoint shardCheckpoint : initialCheckpoint) {
      b.put(shardCheckpoint.getShardId(), shardCheckpoint);
    }
    this.shardsCheckpointsMap = new ConcurrentHashMap<>(b.build());
  }

  @Override
  public void ackRecord(Record record) {
    if (record.getKinesisRecord().isPresent()) {
      shardsCheckpointsMap.computeIfPresent(
          record.getShardId(),
          (k, v) ->
              v.moveAfter(record.getKinesisRecord().get(), record.getContinuationSequenceNumber()));
    } else {
      shardsCheckpointsMap.computeIfPresent(
          record.getShardId(), (k, v) -> v.moveAfter(record.getContinuationSequenceNumber()));
    }
  }

  @Override
  public ShardCheckpoint getCheckpoint(String shardId) {
    return Checkers.checkNotNull(shardsCheckpointsMap.get(shardId), shardId);
  }

  @Override
  public Instant getWatermark() {
    return Instant.EPOCH;
  }

  @Override
  public KinesisReaderCheckpoint getCheckpointMark() {
    return new KinesisReaderCheckpoint(shardsCheckpointsMap.values());
  }
}
