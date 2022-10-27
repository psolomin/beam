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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisClientBuilderStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisClientProxyStubBehaviours;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.kinesis.common.InitialPositionInStream;

@RunWith(MockitoJUnitRunner.class)
public class KinesisEnhancedFanOutReaderTest {
  @Mock private KinesisIO.Read read;

  private KinesisEnhancedFanOutReader reader;

  @After
  public void tearDown() throws IOException {
    reader.close();
  }

  @Test
  public void readsThroughAllDataAvailable() throws IOException {
    when(read.getStreamName()).thenReturn("stream-01");
    when(read.getConsumerArn()).thenReturn("consumer-01");
    when(read.getInitialPosition()).thenReturn(new StartingPoint(InitialPositionInStream.LATEST));

    KinesisClientBuilderStub builder = KinesisClientProxyStubBehaviours.twoShardsWithRecords();
    Iterable<ShardCheckpoint> shardCheckpoints = ImmutableList.of();
    KinesisReaderCheckpoint checkpointMark = new KinesisReaderCheckpoint(shardCheckpoints);
    CheckpointGenerator checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
    KinesisEnhancedFanOutSource source = new KinesisEnhancedFanOutSource(read);
    reader = new KinesisEnhancedFanOutReader(read, builder, checkpointGenerator, source);
    assertThat(reader.start()).isTrue();
    List<KinesisRecord> records = new ArrayList<>();
    // 2 shards * (3 data records + 3 records without data) * 2 re-subscribes
    int fetchAttempts = 24;
    int recordIsPresentCnt = 0;

    for (int i = 0; i < fetchAttempts; i++) {
      if (reader.advance()) {
        recordIsPresentCnt++;
        records.add(reader.getCurrent());
      }
    }
    assertEquals(12, recordIsPresentCnt);
    assertEquals(12, records.size());
  }
}
