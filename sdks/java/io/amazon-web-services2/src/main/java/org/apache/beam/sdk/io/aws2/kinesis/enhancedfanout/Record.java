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

import java.util.Objects;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;

class Record {
  private final String shardId;
  private final Optional<KinesisRecord> kinesisRecord;
  private final String continuationSequenceNumber;
  private final long subSequenceNumber;

  Record(
      String shardId,
      Optional<KinesisRecord> kinesisClientRecord,
      String continuationSequenceNumber,
      long subSequenceNumber) {
    this.shardId = shardId;
    this.kinesisRecord = kinesisClientRecord;
    this.continuationSequenceNumber = continuationSequenceNumber;
    this.subSequenceNumber = subSequenceNumber;
  }

  static Record checkPointOnly(String shardId, String continuationSequenceNumber) {
    return new Record(shardId, Optional.absent(), continuationSequenceNumber, 0L);
  }

  static Record record(String shardId, KinesisRecord kinesisRecord) {
    return new Record(
        shardId,
        Optional.of(kinesisRecord),
        kinesisRecord.getSequenceNumber(),
        kinesisRecord.getSubSequenceNumber());
  }

  String getShardId() {
    return shardId;
  }

  Optional<KinesisRecord> getKinesisRecord() {
    return kinesisRecord;
  }

  String getContinuationSequenceNumber() {
    return continuationSequenceNumber;
  }

  long getSubSequenceNumber() {
    return subSequenceNumber;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record record = (Record) o;
    return shardId.equals(record.shardId)
        && kinesisRecord.equals(record.kinesisRecord)
        && continuationSequenceNumber.equals(record.continuationSequenceNumber)
        && subSequenceNumber == record.subSequenceNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, kinesisRecord, continuationSequenceNumber, subSequenceNumber);
  }
}
