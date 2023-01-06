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

import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.joda.time.Instant;

public class ShardSubscribersPoolImpl implements ShardSubscribersPool {
  private final RecordsBuffer recordsBuffer;

  ShardSubscribersPoolImpl(RecordsBuffer recordsBuffer) {
    this.recordsBuffer = recordsBuffer;
  }

  @Override
  public boolean start() {
    return false;
  }

  @Override
  public boolean stop() {
    return false;
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
    return recordsBuffer.getWatermark();
  }

  @Override
  public KinesisReaderCheckpoint getCheckpointMark() {
    return recordsBuffer.getCheckpointMark();
  }
}
