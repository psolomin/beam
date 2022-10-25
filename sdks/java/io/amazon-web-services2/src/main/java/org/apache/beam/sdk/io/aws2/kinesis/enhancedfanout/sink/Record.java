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
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class Record {
  private final String shardId;
  private final KinesisClientRecord kinesisClientRecord;

  public Record(String shardId, KinesisClientRecord kinesisClientRecord) {
    this.shardId = shardId;
    this.kinesisClientRecord = kinesisClientRecord;
  }

  public String getShardId() {
    return shardId;
  }

  public KinesisClientRecord getKinesisClientRecord() {
    return kinesisClientRecord;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Record record = (Record) o;
    return shardId.equals(record.shardId) && kinesisClientRecord.equals(record.kinesisClientRecord);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, kinesisClientRecord);
  }
}
