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

import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicy;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicyFactory;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.Record;
import org.joda.time.Instant;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class ShardEventsConsumerStateImpl implements ShardEventsConsumerState {
  private final String streamName;
  private final String shardId;
  private final WatermarkPolicy watermarkPolicy;
  private final WatermarkPolicyFactory watermarkPolicyFactory;
  private final WatermarkPolicy latestRecordTimestampPolicy =
      WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

  private ShardCheckpoint shardCheckpoint;

  public ShardEventsConsumerStateImpl(
      String streamName,
      String shardId,
      ShardCheckpoint shardCheckpoint,
      WatermarkPolicy watermarkPolicy,
      WatermarkPolicyFactory watermarkPolicyFactory) {
    this.streamName = streamName;
    this.shardId = shardId;
    this.shardCheckpoint = shardCheckpoint;
    this.watermarkPolicy = watermarkPolicy;
    this.watermarkPolicyFactory = watermarkPolicyFactory;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getShardId() {
    return shardId;
  }

  @Override
  public Instant getShardWatermark() {
    return watermarkPolicy.getWatermark();
  }

  @Override
  public void updateContinuationSequenceNumber(String continuationSequenceNumber) {}

  @Override
  public void ackRecord(Record record, String continuationSequenceNumber) {
    if (record.getKinesisClientRecord().isPresent()) {
      KinesisClientRecord clientRecord = record.getKinesisClientRecord().get();
      KinesisRecord convertedRecord = new KinesisRecord(clientRecord, streamName, shardId);
      shardCheckpoint = shardCheckpoint.moveAfter(convertedRecord, continuationSequenceNumber);
      watermarkPolicy.update(convertedRecord);
      latestRecordTimestampPolicy.update(convertedRecord);
    } else {
      shardCheckpoint = shardCheckpoint.moveAfter(continuationSequenceNumber);
    }
  }

  public ShardCheckpoint getShardCheckpoint() {
    return shardCheckpoint;
  }

  public WatermarkPolicy getWatermarkPolicy() {
    return watermarkPolicy;
  }

  public WatermarkPolicyFactory getWatermarkPolicyFactory() {
    return watermarkPolicyFactory;
  }

  public WatermarkPolicy getLatestRecordTimestampPolicy() {
    return latestRecordTimestampPolicy;
  }
}
