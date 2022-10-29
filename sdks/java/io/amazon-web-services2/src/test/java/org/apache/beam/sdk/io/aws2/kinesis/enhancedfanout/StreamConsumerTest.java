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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisClientBuilderStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisClientProxyStubBehaviours;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.InMemGlobalQueueRecordsSink;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.RecordsSink;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

@RunWith(JUnit4.class)
public class StreamConsumerTest {
  private static final String STREAM_NAME = KinesisClientProxyStubBehaviours.STREAM_NAME;
  private static final String CONSUMER_ARN = KinesisClientProxyStubBehaviours.CONSUMER_ARN;
  private final StartingPoint startingPoint = new StartingPoint(InitialPositionInStream.LATEST);
  private StreamConsumer consumer;

  @After
  public void tearDown() throws InterruptedException {
    consumer.initiateGracefulShutdown();
    consumer.awaitTermination();
    assertFalse(consumer.isRunning());
  }

  private StreamConsumer createConsumer(Config config, ClientBuilder builder) {
    List<ShardCheckpoint> checkpoints =
        ShardsListingUtils.initSubscribedShardsProgressInfo(config, builder);
    KinesisReaderCheckpoint initialCheckpoint = new KinesisReaderCheckpoint(checkpoints);
    RecordsSink sink = new InMemGlobalQueueRecordsSink();
    return StreamConsumer.init(config, builder, initialCheckpoint, sink);
  }

  @Test
  public void consumesAllEventsFromMultipleShards() throws InterruptedException {
    Config config = new Config(STREAM_NAME, CONSUMER_ARN, startingPoint);
    KinesisClientBuilderStub builder = KinesisClientProxyStubBehaviours.twoShardsWithRecords();
    consumer = createConsumer(config, builder);
    List<KinesisRecord> records = waitForRecords(consumer, 12);
    int expectedRecordsCntPerShard = 6;
    checkEventsCnt(
        expectedRecordsCntPerShard,
        ImmutableList.of("shard-000", "shard-001"),
        ImmutableList.of(),
        records);
    // 2 shards x (1 initial subscribe + 2 re-subscribes)
    assertEquals(6, builder.subscribeRequestsSeen().size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeSeqNumber("shard-000", "2"),
            subscribeSeqNumber("shard-001", "2"),
            subscribeSeqNumber("shard-000", "2"),
            subscribeSeqNumber("shard-001", "2"));
    assertTrue(expectedSubscribeRequests.containsAll(builder.subscribeRequestsSeen()));
    assertTrue(consumer.isRunning());
  }

  @Test
  public void consumesAllEventsFromChildShards() throws InterruptedException {
    Config config = new Config(STREAM_NAME, CONSUMER_ARN, startingPoint);
    KinesisClientBuilderStub builder =
        KinesisClientProxyStubBehaviours.twoShardsWithRecordsAndShardUp();

    consumer = createConsumer(config, builder);
    List<KinesisRecord> records = waitForRecords(consumer, 42);
    int expectedRecordsCntPerShard = 7;
    List<String> parentShards = ImmutableList.of("shard-000", "shard-001");
    List<String> childShards = ImmutableList.of("shard-002", "shard-003", "shard-004", "shard-005");
    checkEventsCnt(expectedRecordsCntPerShard, parentShards, childShards, records);
    // 2 shards with initial subscribe + 4 new shards with initial subscribe
    assertEquals(6, builder.subscribeRequestsSeen().size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeSeqNumber("shard-002", "002"),
            subscribeSeqNumber("shard-003", "003"),
            subscribeSeqNumber("shard-004", "004"),
            subscribeSeqNumber("shard-005", "005"));
    assertTrue(expectedSubscribeRequests.containsAll(builder.subscribeRequestsSeen()));
    assertTrue(consumer.isRunning());
  }

  @Test
  public void consumesAllEventsFromChildShardsAfterMerge() throws InterruptedException {
    Config config = new Config(STREAM_NAME, CONSUMER_ARN, startingPoint);
    KinesisClientBuilderStub builder =
        KinesisClientProxyStubBehaviours.fourShardsWithRecordsAndShardDown();

    consumer = createConsumer(config, builder);
    List<KinesisRecord> records = waitForRecords(consumer, 77);
    int expectedRecordsCntPerShard = 11;
    List<String> parentShards =
        ImmutableList.of("shard-000", "shard-001", "shard-002", "shard-003");
    List<String> childShards = ImmutableList.of("shard-004", "shard-005", "shard-006");
    checkEventsCnt(expectedRecordsCntPerShard, parentShards, childShards, records);
    // 4 shards with initial subscribe + 3 new shards with initial subscribe
    int expectedSubscribeRequests = 7;
    assertEquals(expectedSubscribeRequests, builder.subscribeRequestsSeen().size());
    assertTrue(consumer.isRunning());
  }

  @Test
  public void consumersNoEventsFromEmptyShards() throws InterruptedException {
    Config config = new Config(STREAM_NAME, CONSUMER_ARN, startingPoint);
    KinesisClientBuilderStub builder = KinesisClientProxyStubBehaviours.twoShardsEmpty();
    consumer = createConsumer(config, builder);
    List<KinesisRecord> records = waitForRecords(consumer, 0, 6);
    assertTrue(records.isEmpty());
    // 2 shards x (1 initial subscribe + 2 re-subscribes)
    int expectedSubscribeRequests = 6;
    assertEquals(expectedSubscribeRequests, builder.subscribeRequestsSeen().size());
    assertTrue(consumer.isRunning());
  }

  @Test
  public void stopsUponUnRecoverableError() throws InterruptedException {
    Config config = new Config(STREAM_NAME, CONSUMER_ARN, startingPoint);
    KinesisClientBuilderStub builder =
        KinesisClientProxyStubBehaviours.twoShardsWithRecordsOneShardError();

    consumer = createConsumer(config, builder);
    Thread.sleep(500);
    assertFalse(consumer.isRunning());
  }

  @Test
  public void continuesUponRecoverableError() throws InterruptedException {
    Config config = new Config(STREAM_NAME, CONSUMER_ARN, startingPoint);
    KinesisClientBuilderStub builder =
        KinesisClientProxyStubBehaviours.twoShardsWithRecordsOneShardRecoverableError();

    consumer = createConsumer(config, builder);
    List<KinesisRecord> records = waitForRecords(consumer, 20);
    int expectedRecordsCntPerShard = 10;
    checkEventsCnt(
        expectedRecordsCntPerShard,
        ImmutableList.of("shard-000", "shard-001"),
        ImmutableList.of(),
        records);
    // 2 shards x (1 initial subscribe + 2 re-subscribes)
    int expectedSubscribeRequests = 6;
    assertEquals(expectedSubscribeRequests, builder.subscribeRequestsSeen().size());
    assertTrue(consumer.isRunning());
  }

  private static void checkEventsCnt(
      int expectedRecordsCntPerShard,
      List<String> parentShards,
      List<String> childShards,
      List<KinesisRecord> records) {

    Stream.of(parentShards, childShards)
        .flatMap(List::stream)
        .forEach(
            shardId -> {
              List<KinesisRecord> filtered =
                  records.stream()
                      .filter(kr -> kr.getShardId().equals(shardId))
                      .collect(Collectors.toList());
              String msg = String.format("Shard %s did not get all expected records.", shardId);
              assertEquals(msg, expectedRecordsCntPerShard, filtered.size());
            });
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

  private static List<KinesisRecord> waitForRecords(
      StreamConsumer consumer, int expectedRecordsCnt, int maxAttempts) {
    int attemptNo = 0;
    List<KinesisRecord> records = new ArrayList<>();

    while (attemptNo < maxAttempts) {
      attemptNo++;
      CustomOptional<KinesisRecord> maybeRecord = consumer.nextRecord();
      if (maybeRecord.isPresent()) records.add(maybeRecord.get());
    }

    assertEquals(expectedRecordsCnt, records.size());
    return records;
  }

  private static List<KinesisRecord> waitForRecords(
      StreamConsumer consumer, int expectedRecordsCnt) {
    return waitForRecords(consumer, expectedRecordsCnt, expectedRecordsCnt * 3);
  }
}
