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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class InMemCollectionRecordsSink implements RecordsSink {
  private final Map<String, Queue<KinesisClientRecord>> events;

  public InMemCollectionRecordsSink() {
    this.events = new ConcurrentHashMap<>();
  }

  @Override
  public void submit(
      String shardId, Optional<KinesisClientRecord> record, String continuationSequenceNumber) {
    if (record.isPresent()) addSingleRecord(shardId, record.get());
  }

  @Override
  public void submit(
      String shardId, List<KinesisClientRecord> records, String continuationSequenceNumber) {
    if (records.isEmpty()) submit(shardId, Optional.absent(), continuationSequenceNumber);
    else records.forEach(r -> submit(shardId, Optional.of(r), continuationSequenceNumber));
  }

  @Override
  public long getTotalCnt() {
    return events.values().stream().map(q -> (long) q.size()).reduce(0L, Long::sum);
  }

  @Override
  public Record fetch() {
    return null;
  }

  public List<KinesisClientRecord> getAllRecords(String shardId) {
    return new ArrayList<>(events.get(shardId));
  }

  public Set<String> shardsSeen() {
    return events.keySet();
  }

  private void addSingleRecord(String shardId, KinesisClientRecord r) {
    events.computeIfAbsent(shardId, k -> new ArrayDeque<>()).add(r);
  }
}
