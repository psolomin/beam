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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.RecordsGenerators.createClientRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.RecordsGenerators.waitForRecords;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class InMemGlobalQueueRecordsSinkTest {
  private RecordsSink sink;

  @Before
  public void setUp() {
    sink = createSink();
  }

  private RecordsSink createSink() {
    return new InMemGlobalQueueRecordsSink();
  }

  @Test
  public void submittedRecordsCanBeFetched() {
    int nRecordsTotal = 20;
    List<KinesisClientRecord> records = createClientRecords(nRecordsTotal);
    sink.submitMany("shard-000", records, "0123");
    List<Record> recordsReceived = waitForRecords(sink, nRecordsTotal, nRecordsTotal);
    assertEquals(nRecordsTotal, recordsReceived.size());
    assertEquals(Optional.absent(), sink.fetch());
  }

  @Test
  public void submittedRecordsFromMultipleThreadsCanBeFetched() throws InterruptedException {
    int nThreads = 4;
    int nRecordsPerShard = 20;
    int recordsTotal = nThreads * nRecordsPerShard;
    ExecutorService executor = Executors.newFixedThreadPool(nThreads);
    List<Callable<Object>> tasks = generateTasks(sink, nThreads, nRecordsPerShard);
    executor.invokeAll(tasks);
    executor.shutdown();
    List<Record> recordsReceived = waitForRecords(sink, recordsTotal, recordsTotal);
    assertEquals(recordsTotal, recordsReceived.size());
    assertEquals(Optional.absent(), sink.fetch());
  }

  @Test
  public void submittedRecordsWithoutDataCanBeFetched() {
    List<KinesisClientRecord> records = ImmutableList.of();
    sink.submitMany("shard-000", records, "0123");
    Record expectedRecord = new Record("shard-000", Optional.absent(), "0123");
    assertEquals(Optional.of(expectedRecord), sink.fetch());
    assertEquals(Optional.absent(), sink.fetch());
  }

  private static List<Callable<Object>> generateTasks(
      RecordsSink sink, final int nThreads, final int nRecordsPerShard) {
    String seqNumber = "0123";
    List<Callable<Object>> tasks = new ArrayList<>();
    for (int i = 0; i < nThreads; i++) {
      int finalI = i;
      tasks.add(
          () -> {
            sink.submitMany(
                Integer.toString(finalI), createClientRecords(nRecordsPerShard), seqNumber);
            return null;
          });
    }
    return tasks;
  }
}
