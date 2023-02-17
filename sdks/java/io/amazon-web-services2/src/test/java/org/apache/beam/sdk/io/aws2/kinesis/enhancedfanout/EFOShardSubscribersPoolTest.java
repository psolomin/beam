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

import static junit.framework.TestCase.assertTrue;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Helpers.createConfig;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Helpers.createReadSpec;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.RecordsGenerators.eventWithOutRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.RecordsGenerators.eventWithRecords;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class EFOShardSubscribersPoolTest {
  @Test
  public void poolReSubscribesAndReadsRecords() throws Exception {
    Config config = createConfig();
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis = new StubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(10));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(11));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(12));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3, 5));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(8));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(9));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(11));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(config, readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 18);
    assertEquals(18, actualRecords.size());
    kinesis.close();
    assertTrue(pool.stop());

    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeAfterSeqNumber("shard-000", "2"),
            Helpers.subscribeAfterSeqNumber("shard-001", "2"),
            Helpers.subscribeAfterSeqNumber("shard-000", "9"),
            Helpers.subscribeAfterSeqNumber("shard-001", "7"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
  }

  static List<KinesisRecord> waitForRecords(EFOShardSubscribersPool pool, int expectedCnt)
      throws Exception {
    List<KinesisRecord> records = new ArrayList<>();
    int i = 0;
    while (i < expectedCnt) {
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
