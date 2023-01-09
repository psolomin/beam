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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisClientBuilderStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.KinesisClientProxyStubBehaviours;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class ShardSubscribersPoolImplTest {
  @Test
  public void poolReadsRecords() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientBuilderStub clientBuilder =
        KinesisClientProxyStubBehaviours.twoShardsWithRecords();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(clientBuilder);
    ShardSubscribersPoolState state = new ShardSubscribersPoolStateImpl(initialCheckpoint);
    RecordsBuffer buffer = new RecordsBufferImpl(config, state);
    ShardSubscribersPool pool = new ShardSubscribersPoolImpl(config, clientBuilder, state, buffer);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 12);
    assertEquals(12, actualRecords.size());
    // 2 shards x (1 initial subscribe + 1 re-subscribe)
    assertEquals(6, clientBuilder.subscribeRequestsSeen().size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeSeqNumber("shard-000", "2"),
            subscribeSeqNumber("shard-001", "2"),
            subscribeSeqNumber("shard-000", "2"),
            subscribeSeqNumber("shard-001", "2"));
    assertTrue(expectedSubscribeRequests.containsAll(clientBuilder.subscribeRequestsSeen()));
    assertTrue(pool.isRunning());
    assertTrue(pool.stop());
  }

  @Test
  public void poolReadsRecordsAfterTransientError() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientBuilderStub clientBuilder =
        KinesisClientProxyStubBehaviours.twoShardsWithRecordsOneShardRecoverableError();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(clientBuilder);
    ShardSubscribersPoolState state = new ShardSubscribersPoolStateImpl(initialCheckpoint);
    RecordsBuffer buffer = new RecordsBufferImpl(config, state);
    ShardSubscribersPool pool = new ShardSubscribersPoolImpl(config, clientBuilder, state, buffer);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = waitForRecords(pool, 20);
    assertEquals(20, actualRecords.size());
    // 2 shards, 2 initial subscribes, 2
    assertEquals(6, clientBuilder.subscribeRequestsSeen().size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeSeqNumber("shard-000", "4"),
            subscribeSeqNumber("shard-001", "4"),
            subscribeSeqNumber("shard-000", "4"),
            subscribeSeqNumber("shard-001", "4"));
    assertTrue(expectedSubscribeRequests.containsAll(clientBuilder.subscribeRequestsSeen()));
    assertTrue(pool.isRunning());
    assertTrue(pool.stop());
  }

  @Test
  public void poolStopsUponUnRecoverableError() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientBuilderStub clientBuilder =
        KinesisClientProxyStubBehaviours.twoShardsWithRecordsOneShardError();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(clientBuilder);
    ShardSubscribersPoolState state = new ShardSubscribersPoolStateImpl(initialCheckpoint);
    RecordsBuffer buffer = new RecordsBufferImpl(config, state);
    ShardSubscribersPool pool = new ShardSubscribersPoolImpl(config, clientBuilder, state, buffer);
    assertTrue(pool.start());
    // error in one shard will stop others, hence the final count is not deterministic
    List<KinesisRecord> actualRecords = waitForRecords(pool, 15);
    assertTrue(actualRecords.size() <= 15);
    // 2 shards - (2 initial subscribes + 1 re-subscribe)
    assertEquals(3, clientBuilder.subscribeRequestsSeen().size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeSeqNumber("shard-000", "4"));
    assertTrue(expectedSubscribeRequests.containsAll(clientBuilder.subscribeRequestsSeen()));
    assertFalse(pool.isRunning());
    assertTrue(pool.stop());
  }

  public static List<KinesisRecord> waitForRecords(ShardSubscribersPool pool, int expectedCnt) {
    List<KinesisRecord> records = new ArrayList<>();
    int maxAttempts = expectedCnt * 3;
    int i = 0;
    while (i < maxAttempts) {
      CustomOptional<KinesisRecord> r = pool.nextRecord();
      if (r.isPresent()) records.add(r.get());
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
}
