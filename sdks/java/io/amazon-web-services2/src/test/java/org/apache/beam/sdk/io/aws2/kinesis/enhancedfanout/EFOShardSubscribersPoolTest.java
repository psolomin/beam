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
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Helpers.createReadSpec;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.RecordsGenerators.eventWithOutRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.RecordsGenerators.eventWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.RecordsGenerators.shardUpEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.netty.handler.timeout.ReadTimeoutException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class EFOShardSubscribersPoolTest {
  @Test
  public void poolReSubscribesAndReadsRecords() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
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
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 26);
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
    List<ShardCheckpoint> expectedCheckPoint =
        ImmutableList.of(
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "9", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "11", 0L));
    KinesisReaderCheckpoint actualCheckPoint = pool.getCheckpointMark();
    assertEquals(expectedCheckPoint, ImmutableList.copyOf(actualCheckPoint.iterator()));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
  }

  @Test
  public void poolReSubscribesWhenNoRecordsCome() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(31));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(32));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(33));

    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(8));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(9));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(11));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 6);
    assertEquals(0, actualRecords.size());
    kinesis.close();
    assertTrue(pool.stop());

    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeAfterSeqNumber("shard-000", "31"),
            Helpers.subscribeAfterSeqNumber("shard-001", "8"),
            Helpers.subscribeAfterSeqNumber("shard-000", "32"),
            Helpers.subscribeAfterSeqNumber("shard-001", "9"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
  }

  @Test
  public void poolReSubscribesWhenRecoverableErrorOccurs() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(3))
        .failWith(new ReadTimeoutException());

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(10));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(11));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(12));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis
        .stubSubscribeToShard("shard-001", eventWithRecords(3, 5))
        .failWith(SdkClientException.create("this is recoverable", new ReadTimeoutException()));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(8));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(9));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(11));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis, 1L);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 25);
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
    List<ShardCheckpoint> expectedCheckPoint =
        ImmutableList.of(
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "11", 0L));
    KinesisReaderCheckpoint actualCheckPoint = pool.getCheckpointMark();
    assertEquals(expectedCheckPoint, ImmutableList.copyOf(actualCheckPoint.iterator()));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
  }

  @Test
  public void poolFailsWhenNonRecoverableErrorOccurs() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(3))
        .failWith(new RuntimeException("Oh..."));

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(10));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(11));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(12));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(8));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(9));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(11));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    Throwable exception = assertThrows(IOException.class, () -> waitForRecords(pool, 10));
    assertEquals("java.lang.RuntimeException: Oh...", exception.getMessage());
    assertTrue(exception.getCause() instanceof RuntimeException);
    kinesis.close();
    assertTrue(pool.stop());
  }

  @Test
  public void poolFailsWhenConsumerDoesNotExist() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(3))
        .failWith(
            new CompletionException(
                "Err ...",
                ResourceNotFoundException.builder()
                    .cause(null)
                    .awsErrorDetails(
                        AwsErrorDetails.builder()
                            .serviceName("Kinesis")
                            .errorCode("ResourceNotFoundException")
                            .errorMessage("Consumer consumer-01 not found.")
                            .build())
                    .build()));

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(10));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(11));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(12));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(8));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(9));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(11));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    Throwable exception = assertThrows(IOException.class, () -> waitForRecords(pool, 10));
    assertEquals("java.util.concurrent.CompletionException: Err ...", exception.getMessage());
    Throwable cause = exception.getCause().getCause();
    assertTrue(cause instanceof ResourceNotFoundException);
    assertEquals(
        "Consumer consumer-01 not found. (Service: Kinesis, Status Code: 0, Request ID: null)",
        cause.getMessage());
    kinesis.close();
    assertTrue(pool.stop());
  }

  @Test
  public void poolHandlesShardUp() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(10));
    kinesis.stubSubscribeToShard(
        "shard-000",
        shardUpEvent(ImmutableList.of("shard-000"), ImmutableList.of("shard-002", "shard-003")));
    kinesis.stubSubscribeToShard("shard-002", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventWithOutRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard("shard-002", eventWithOutRecords(7));
    kinesis.stubSubscribeToShard("shard-003", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-003", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard("shard-003", eventWithOutRecords(7));
    kinesis.stubSubscribeToShard("shard-003", eventWithOutRecords(8));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3, 5));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(8));
    kinesis.stubSubscribeToShard(
        "shard-001", shardUpEvent(ImmutableList.of("shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-004", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-004", eventWithOutRecords(5));
    kinesis.stubSubscribeToShard("shard-004", eventWithOutRecords(6));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 50);
    assertEquals(34, actualRecords.size());
    kinesis.close();
    assertTrue(pool.stop());

    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeAfterSeqNumber("shard-000", "2"),
            Helpers.subscribeAfterSeqNumber("shard-001", "2"),
            Helpers.subscribeAfterSeqNumber("shard-000", "9"),
            Helpers.subscribeAfterSeqNumber("shard-001", "7"),
            Helpers.subscribeTrimHorizon("shard-002"),
            Helpers.subscribeTrimHorizon("shard-003"),
            Helpers.subscribeTrimHorizon("shard-004"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));

    List<ShardCheckpoint> expectedCheckPoint =
        ImmutableList.of(
            new ShardCheckpoint(
                "stream-01", "shard-002", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "6", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-004", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "5", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-003", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "7", 0L));
    KinesisReaderCheckpoint actualCheckPoint = pool.getCheckpointMark();
    assertEquals(expectedCheckPoint, ImmutableList.copyOf(actualCheckPoint.iterator()));
  }

  @Test
  public void poolHandlesShardDown() throws Exception {
    KinesisIO.Read readSpec = createReadSpec();
    StubbedKinesisAsyncClient kinesis =
        new StubbedKinesisAsyncClient(
            10, ImmutableList.of("shard-000", "shard-001", "shard-002", "shard-003"));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventWithOutRecords(10));
    kinesis.stubSubscribeToShard(
        "shard-000",
        shardUpEvent(ImmutableList.of("shard-000", "shard-001"), ImmutableList.of("shard-004")));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(5));
    kinesis.stubSubscribeToShard("shard-001", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard(
        "shard-001",
        shardUpEvent(ImmutableList.of("shard-000", "shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-002", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventWithOutRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard(
        "shard-002",
        shardUpEvent(ImmutableList.of("shard-002", "shard-003"), ImmutableList.of("shard-005")));

    kinesis.stubSubscribeToShard("shard-003", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-003", eventWithOutRecords(5));
    kinesis.stubSubscribeToShard("shard-003", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard(
        "shard-003",
        shardUpEvent(ImmutableList.of("shard-002", "shard-003"), ImmutableList.of("shard-005")));

    kinesis.stubSubscribeToShard("shard-004", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-004", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard("shard-004", eventWithOutRecords(7));
    kinesis.stubSubscribeToShard("shard-004", eventWithOutRecords(8));

    kinesis.stubSubscribeToShard("shard-005", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-005", eventWithOutRecords(6));
    kinesis.stubSubscribeToShard("shard-005", eventWithOutRecords(7));
    kinesis.stubSubscribeToShard("shard-005", eventWithOutRecords(8));

    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(readSpec).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 60);
    assertEquals(37, actualRecords.size());
    kinesis.close();
    assertTrue(pool.stop());

    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeLatest("shard-002"),
            Helpers.subscribeLatest("shard-003"),
            Helpers.subscribeTrimHorizon("shard-004"),
            Helpers.subscribeTrimHorizon("shard-005"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));

    List<ShardCheckpoint> expectedCheckPoint =
        ImmutableList.of(
            new ShardCheckpoint(
                "stream-01", "shard-005", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "7", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-004", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L));
    KinesisReaderCheckpoint actualCheckPoint = pool.getCheckpointMark();
    assertEquals(expectedCheckPoint, ImmutableList.copyOf(actualCheckPoint.iterator()));
  }

  static List<KinesisRecord> waitForRecords(EFOShardSubscribersPool pool, int maxAttempts)
      throws Exception {
    List<KinesisRecord> records = new ArrayList<>();
    int i = 0;
    while (i < maxAttempts) {
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
