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
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisClientProxyStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisStubBehaviours;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class ShardSubscribersPoolImplTest {
  @Test
  public void poolReSubscribesAndReadsRecords() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecords();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 12);
    assertEquals(12, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeSeqNumber("shard-000", "6"),
            subscribeSeqNumber("shard-001", "12"),
            subscribeSeqNumber("shard-000", "18"),
            subscribeSeqNumber("shard-001", "24"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    assertTrue(pool.stop());
  }

  @Test
  public void poolHandlesShardUp() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecordsAndShardUp();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 70);
    assertEquals(70, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeTrimHorizon("shard-002"),
            subscribeTrimHorizon("shard-003"),
            subscribeTrimHorizon("shard-004"),
            subscribeTrimHorizon("shard-005"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    assertTrue(pool.stop());
  }

  @Test
  public void poolHandlesShardDown() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.fourShardsWithRecordsAndShardDown();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 77);

    assertEquals(77, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeLatest("shard-002"),
            subscribeLatest("shard-003"),
            subscribeTrimHorizon("shard-004"),
            subscribeTrimHorizon("shard-005"),
            subscribeTrimHorizon("shard-006"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
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

  private static SubscribeToShardRequest subscribeLatest(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
        .build();
  }

  private static SubscribeToShardRequest subscribeSeqNumber(String shardId, String seqNumber) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(
            StartingPosition.builder()
                .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(seqNumber)
                .build())
        .build();
  }

  private static SubscribeToShardRequest subscribeTrimHorizon(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.TRIM_HORIZON).build())
        .build();
  }
}
