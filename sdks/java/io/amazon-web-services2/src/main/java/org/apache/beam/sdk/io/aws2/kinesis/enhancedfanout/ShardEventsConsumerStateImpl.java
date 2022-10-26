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

import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicy;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicyFactory;
import org.joda.time.Instant;

public class ShardEventsConsumerStateImpl implements ShardEventsConsumerState {
  private final String shardId;
  private final ShardCheckpoint shardCheckpoint;
  private final WatermarkPolicy watermarkPolicy;
  private final WatermarkPolicyFactory watermarkPolicyFactory;
  private final WatermarkPolicy latestRecordTimestampPolicy =
      WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

  public ShardEventsConsumerStateImpl(
      String shardId,
      ShardCheckpoint shardCheckpoint,
      WatermarkPolicy watermarkPolicy,
      WatermarkPolicyFactory watermarkPolicyFactory) {
    this.shardId = shardId;
    this.shardCheckpoint = shardCheckpoint;
    this.watermarkPolicy = watermarkPolicy;
    this.watermarkPolicyFactory = watermarkPolicyFactory;
  }

  public String getShardId() {
    return shardId;
  }

  @Override
  public Instant getShardWatermark() {
    return watermarkPolicy.getWatermark();
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
