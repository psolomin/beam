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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.joda.time.Duration;
import org.joda.time.Instant;

@SuppressWarnings("UnusedVariable")
public class KinesisEnhancedFanOutReader extends UnboundedSource.UnboundedReader<KinesisRecord> {
  private final KinesisIO.Read spec;
  private final SimplifiedKinesisClient kinesis;
  private final KinesisEnhancedFanOutSource source;
  private final CheckpointGenerator checkpointGenerator;
  private final Duration backlogBytesCheckThreshold;

  KinesisEnhancedFanOutReader(
      KinesisIO.Read spec,
      SimplifiedKinesisClient kinesis,
      CheckpointGenerator initialCheckpointGenerator,
      KinesisEnhancedFanOutSource source) {
    this(spec, kinesis, initialCheckpointGenerator, source, Duration.standardSeconds(30));
  }

  KinesisEnhancedFanOutReader(
      KinesisIO.Read spec,
      SimplifiedKinesisClient kinesis,
      CheckpointGenerator checkpointGenerator,
      KinesisEnhancedFanOutSource source,
      Duration backlogBytesCheckThreshold) {
    this.spec = checkNotNull(spec, "spec");
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.checkpointGenerator = checkNotNull(checkpointGenerator, "checkpointGenerator");
    this.source = source;
    this.backlogBytesCheckThreshold = backlogBytesCheckThreshold;
  }

  @Override
  public boolean start() throws IOException {
    return false;
  }

  @Override
  public boolean advance() throws IOException {
    return false;
  }

  @Override
  public KinesisRecord getCurrent() throws NoSuchElementException {
    return null;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return null;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Instant getWatermark() {
    return null;
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return null;
  }

  @Override
  public UnboundedSource<KinesisRecord, ?> getCurrentSource() {
    return source;
  }
}
