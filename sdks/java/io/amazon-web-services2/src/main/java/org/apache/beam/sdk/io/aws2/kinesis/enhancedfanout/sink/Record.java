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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink;

import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class Record {
  private final String shardId;
  private final Optional<KinesisClientRecord> kinesisClientRecord;
  private final String continuationSequenceNumber;

  public Record(
      String shardId,
      Optional<KinesisClientRecord> kinesisClientRecord,
      String continuationSequenceNumber) {
    this.shardId = shardId;
    this.kinesisClientRecord = kinesisClientRecord;
    this.continuationSequenceNumber = continuationSequenceNumber;
  }

  public String getShardId() {
    return shardId;
  }

  public Optional<KinesisClientRecord> getKinesisClientRecord() {
    return kinesisClientRecord;
  }

  public String getContinuationSequenceNumber() {
    return continuationSequenceNumber;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Record record = (Record) o;
    return shardId.equals(record.shardId)
        && kinesisClientRecord.equals(record.kinesisClientRecord)
        && continuationSequenceNumber.equals(record.continuationSequenceNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, kinesisClientRecord, continuationSequenceNumber);
  }
}
