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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.Helpers.createConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisClientProxyStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisStubBehaviours;
import org.junit.Test;

public class ShardSubscribersPoolImplTest {
  @Test
  public void test() {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecords();
    List<String> shardsIds = Arrays.asList("shard-000", "shard-001");
    ShardSubscribersPoolImpl pool = new ShardSubscribersPoolImpl(config, kinesis, shardsIds);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 12);
    assertEquals(12, actualRecords.size());
    assertTrue(pool.stop());
  }

  public static List<KinesisRecord> waitForRecords(ShardSubscribersPoolImpl pool, int expectedCnt) {
    List<KinesisRecord> records = new ArrayList<>();
    int maxAttempts = expectedCnt * 3;
    int i = 0;
    while (i < maxAttempts) {
      CustomOptional<KinesisRecord> r = pool.nextRecord();
      if (r.isPresent()) {
        records.add(r.get());
      }
      i++;
    }
    return records;
  }
}
