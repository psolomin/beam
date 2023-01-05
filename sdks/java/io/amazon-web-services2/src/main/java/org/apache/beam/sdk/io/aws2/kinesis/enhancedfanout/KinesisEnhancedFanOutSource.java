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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecordCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Checkers.checkNotNull;

public class KinesisEnhancedFanOutSource
    extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {

  private final KinesisIO.Read spec;
  private final CheckpointGenerator checkpointGenerator;

  public KinesisEnhancedFanOutSource(KinesisIO.Read read) {
    this(
        read,
        new DynamicCheckpointGenerator(
                checkNotNull(read.getStreamName(), "streamName"),
                checkNotNull(read.getConsumerArn(), "consumerArn"),
                checkNotNull(read.getInitialPosition(), "initialPosition"))
    );
  }

  private KinesisEnhancedFanOutSource(KinesisIO.Read spec, CheckpointGenerator initialCheckpoint) {
    this.spec = checkNotNull(spec, "spec");
    this.checkpointGenerator = checkNotNull(initialCheckpoint, "initialCheckpoint");
  }

  @Override
  public List<KinesisEnhancedFanOutSource> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {
    return ImmutableList.of();
  }

  @Override
  public UnboundedReader<KinesisRecord> createReader(
      PipelineOptions options, @Nullable KinesisReaderCheckpoint checkpointMark)
      throws IOException {
    CheckpointGenerator checkpointGenerator = this.checkpointGenerator;
    if (checkpointMark != null) {
      checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
    }

    ClientBuilder builder = new ClientBuilderImpl();
    KinesisEnhancedFanOutSource source = new KinesisEnhancedFanOutSource(spec);
    return new KinesisEnhancedFanOutReader(spec, builder, checkpointGenerator, source);
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }
}
