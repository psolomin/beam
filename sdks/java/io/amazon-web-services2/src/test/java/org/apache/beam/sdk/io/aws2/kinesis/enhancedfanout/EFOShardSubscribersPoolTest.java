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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Helpers.createConfig;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Helpers.createReadSpec;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.RecordsGenerators.eventWithRecords;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.junit.Test;

public class EFOShardSubscribersPoolTest {
  @Test
  public void poolReSubscribesAndReadsRecords() throws Exception {
    Config config = createConfig();
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis = new StubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(config, readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 3);
    assertEquals(1, actualRecords.size());
    kinesis.close();
  }

  static List<KinesisRecord> waitForRecords(EFOShardSubscribersPool pool, int expectedCnt)
      throws Exception {
    List<KinesisRecord> records = new ArrayList<>();
    int i = 0;
    while (i < expectedCnt - 1) {
      Thread.sleep(20);
      KinesisRecord r = pool.getNextRecord();
      if (r != null) {
        records.add(r);
      }
      i++;
    }
    return records;
  }
}
