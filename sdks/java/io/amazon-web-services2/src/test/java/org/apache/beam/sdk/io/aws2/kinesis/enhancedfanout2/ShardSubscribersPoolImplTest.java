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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.Helpers.createReadSpec;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisClientProxyStubBehaviours;
import org.junit.Test;

public class ShardSubscribersPoolImplTest {
  @Test
  public void poolHasRecords() throws TransientKinesisException {
    KinesisIO.Read readSpec = createReadSpec();
    Config config = Config.fromIOSpec(readSpec);
    ClientBuilder clientBuilder = KinesisClientProxyStubBehaviours.twoShardsWithRecords();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(clientBuilder);
    RecordsBuffer buffer = new RecordsBufferImpl(new RecordsBufferStateImpl(initialCheckpoint));
    ShardSubscribersPool pool = new ShardSubscribersPoolImpl(config, clientBuilder, buffer);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 12);
    assertEquals(12, actualRecords.size());
    assertTrue(pool.stop());
  }

  public static List<KinesisRecord> waitForRecords(ShardSubscribersPool pool, int expectedCnt) {
    List<KinesisRecord> records = new ArrayList<>();
    int maxAttempts = expectedCnt * 3;
    int i = 0;
    while (i < maxAttempts && records.size() < expectedCnt) {
      CustomOptional<KinesisRecord> r = pool.nextRecord();
      if (r.isPresent()) records.add(r.get());
      i++;
    }
    return records;
  }
}
