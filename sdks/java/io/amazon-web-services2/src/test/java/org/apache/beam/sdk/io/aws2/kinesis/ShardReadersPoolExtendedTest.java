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
import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.testRecords;
import static org.assertj.core.api.Assertions.assertThat;

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

  @Before
  public void setUp() {
    SimplifiedKinesisClient simplifiedKinesisClient =
        new SimplifiedKinesisClient(() -> kinesis, () -> cloudWatch, GET_RECORDS_LIMIT);

    KinesisIO.Read readSpec =
        KinesisIO.read()
            .withStreamName(STREAM)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 0L)));
    shardReadersPool = new ShardReadersPool(readSpec, simplifiedKinesisClient, initialCheckpoint);
  }

  @Test
  public void testNextRecordReturnsRecords() throws TransientKinesisException {
    List<List<Record>> records = testRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);

    shardReadersPool.start();

    // before fetching anything:
    KinesisReaderCheckpoint checkpoint0 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint0.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 0L));

    // first record - record with seq num = 0 is skipped:
    CustomOptional<KinesisRecord> record1 = shardReadersPool.nextRecord();
    assertThat(record1.isPresent()).isTrue();
    assertThat(record1.get().getSequenceNumber()).isEqualTo("1");
    KinesisReaderCheckpoint checkpoint1 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint1.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "1", 0L));

    // second record:
    CustomOptional<KinesisRecord> record2 = shardReadersPool.nextRecord();
    assertThat(record2.isPresent()).isTrue();
    assertThat(record2.get().getSequenceNumber()).isEqualTo("2");
    KinesisReaderCheckpoint checkpoint2 = shardReadersPool.getCheckpointMark();
    assertThat(checkpoint2.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "2", 0L));

    // nothing else to fetch:
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @After
  public void clean() {
    shardReadersPool.stop();
  }
}
