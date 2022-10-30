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

import java.util.List;
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
    List<KinesisClientRecord> records = createClientRecords(100);
    sink.submit("shard-000", records, "0123");
    List<Record> recordsReceived = waitForRecords(sink, 100, 100);
    assertEquals(100, recordsReceived.size());
    assertEquals(Optional.absent(), sink.fetch());
  }

  @Test
  public void submittedRecordsWithoutDataCanBeFetched() {
    List<KinesisClientRecord> records = ImmutableList.of();
    sink.submit("shard-000", records, "0123");
    Record expectedRecord = new Record("shard-000", Optional.absent(), "0123");
    assertEquals(Optional.of(expectedRecord), sink.fetch());
    assertEquals(Optional.absent(), sink.fetch());
  }
}
