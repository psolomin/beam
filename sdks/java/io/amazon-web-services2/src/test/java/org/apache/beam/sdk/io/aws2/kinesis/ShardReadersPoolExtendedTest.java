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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.mockRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.mockShardIterators;
import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.testAggregatedRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.testRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStream;

/** Tests {@link ShardReadersPool} with less mocks. */
@RunWith(MockitoJUnitRunner.class)
public class ShardReadersPoolExtendedTest {
  private static final String STREAM = "stream-0";
  private static final String SHARD_0 = "0";
  private static final int GET_RECORDS_LIMIT = 100;

  @Mock private KinesisClient kinesis;
  @Mock private CloudWatchClient cloudWatch;
  private ShardReadersPool shardReadersPool;
  private SimplifiedKinesisClient simplifiedKinesisClient;

  @Before
  public void setUp() {
    simplifiedKinesisClient =
        new SimplifiedKinesisClient(() -> kinesis, () -> cloudWatch, GET_RECORDS_LIMIT);
  }

  @Test
  public void testNextRecordReturnsRecords() throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, "0", 0L)));
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = testRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);

    shardReadersPool.start();

    // before fetching anything:
    KinesisReaderCheckpoint checkpoint0 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint0.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, "0", 0L));

    // record with seq num = 0 is skipped
    for (int i = 1; i < 3L; i++) {
      KinesisRecord kinesisRecord = shardReadersPool.nextRecord().get();
      assertThat(kinesisRecord.getSequenceNumber()).isEqualTo(String.valueOf(i));
      assertThat(kinesisRecord.getSubSequenceNumber()).isEqualTo(0L);
      assertThat(shardReadersPool.getCheckpointMark())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(
                  STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, String.valueOf(i), 0L));
    }
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsRecordsWhenCheckpointIsLegacy()
      throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 0L)));
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = testRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);

    shardReadersPool.start();

    // before fetching anything:
    KinesisReaderCheckpoint checkpoint0 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint0.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 0L));

    // record with seq num = 0 is skipped
    for (int i = 1; i < 3L; i++) {
      KinesisRecord kinesisRecord = shardReadersPool.nextRecord().get();
      assertThat(kinesisRecord.getSequenceNumber()).isEqualTo(String.valueOf(i));
      assertThat(kinesisRecord.getSubSequenceNumber()).isEqualTo(0L);
      assertThat(shardReadersPool.getCheckpointMark())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(
                  STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, String.valueOf(i), 0L));
    }
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  /**
   * This case may happen when {@link EFOShardSubscribersPool} stores its checkpoints.
   *
   * <p>{@link EFOShardSubscribersPool} state re-sets subSequenceNumber only when next record with
   * zero sub-sequence number arrives. Heartbeat records don't reset internal subSequenceNumber.
   *
   * @throws TransientKinesisException
   */
  @Test
  public void testNextRecordReturnsNonAggregatedRecordsIfSeqNumIsPositive()
      throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, "0", 125L)));
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = testRecords(1, 4);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 4);

    shardReadersPool.start();

    for (int i = 1; i < 4L; i++) {
      KinesisRecord kinesisRecord = shardReadersPool.nextRecord().get();
      assertThat(kinesisRecord.getSequenceNumber()).isEqualTo(String.valueOf(i));
      assertThat(kinesisRecord.getSubSequenceNumber()).isEqualTo(0L);
      assertThat(shardReadersPool.getCheckpointMark())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(
                  STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, String.valueOf(i), 0L));
    }
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsDeAggregatedRecords() throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, new StartingPoint(InitialPositionInStream.LATEST))));
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = testAggregatedRecords(1, 6);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 1);

    shardReadersPool.start();

    // before fetching anything:
    KinesisReaderCheckpoint checkpoint0 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint0.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, SHARD_0, new StartingPoint(InitialPositionInStream.LATEST)));

    // check first 3 records
    KinesisReaderCheckpoint intermediateCheckpoint = null;
    for (long i = 0; i < 3L; i++) {
      KinesisRecord kinesisRecord = shardReadersPool.nextRecord().get();
      assertThat(kinesisRecord.getSequenceNumber()).isEqualTo("0");
      assertThat(kinesisRecord.getSubSequenceNumber()).isEqualTo(i);
      intermediateCheckpoint = shardReadersPool.getCheckpointMark();
      assertThat(intermediateCheckpoint.iterator())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, "0", i));
    }

    // re-initialize pool from the previous checkpoint
    shardReadersPool.stop();
    shardReadersPool = initPool(intermediateCheckpoint);
    shardReadersPool.start();

    // 4th record:
    CustomOptional<KinesisRecord> record4 = shardReadersPool.nextRecord();
    assertThat(record4.isPresent()).isTrue();
    assertThat(record4.get().getSequenceNumber()).isEqualTo("0");
    assertThat(record4.get().getSubSequenceNumber()).isEqualTo(3L);
  }

  @Test
  public void testNextRecordReturnsDeAggregatedRecordsWhenStartedAtSeqNum()
      throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, "0", 2L)));

    List<List<Record>> records = testAggregatedRecords(1, 6);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 1);
    shardReadersPool = initPool(initialCheckpoint);
    shardReadersPool.start();

    // before fetching anything:
    KinesisReaderCheckpoint checkpoint0 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint0.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AT_SEQUENCE_NUMBER, "0", 2L));

    // 4th record:
    CustomOptional<KinesisRecord> record4 = shardReadersPool.nextRecord();
    assertThat(record4.isPresent()).isTrue();
    assertThat(record4.get().getSequenceNumber()).isEqualTo("0");
    assertThat(record4.get().getSubSequenceNumber()).isEqualTo(3L);
  }

  @After
  public void clean() throws Exception {
    shardReadersPool.stop();
    simplifiedKinesisClient.close();
    verify(kinesis).close();
    verifyNoInteractions(cloudWatch);
  }

  private static KinesisIO.Read spec() {
    return KinesisIO.read().withStreamName(STREAM);
  }

  private ShardReadersPool initPool(KinesisReaderCheckpoint initialCheckpoint) {
    return new ShardReadersPool(spec(), simplifiedKinesisClient, initialCheckpoint);
  }
}
