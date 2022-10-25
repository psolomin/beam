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
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.LogCountRecordsSink;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.RecordsSink;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnusedVariable")
public class KinesisEnhancedFanOutReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisEnhancedFanOutReader.class);

  private final KinesisIO.Read spec;
  private final ClientBuilder clientBuilder;
  private final KinesisEnhancedFanOutSource source;
  private final CheckpointGenerator checkpointGenerator;
  private final Duration backlogBytesCheckThreshold;

  private CustomOptional<KinesisRecord> currentRecord = CustomOptional.absent();
  private StreamConsumer streamConsumer;

  KinesisEnhancedFanOutReader(
      KinesisIO.Read spec,
      ClientBuilder clientBuilder,
      CheckpointGenerator initialCheckpointGenerator,
      KinesisEnhancedFanOutSource source) {
    this(spec, clientBuilder, initialCheckpointGenerator, source, Duration.standardSeconds(30));
  }

  KinesisEnhancedFanOutReader(
      KinesisIO.Read spec,
      ClientBuilder clientBuilder,
      CheckpointGenerator checkpointGenerator,
      KinesisEnhancedFanOutSource source,
      Duration backlogBytesCheckThreshold) {
    this.spec = checkNotNull(spec, "spec");
    this.clientBuilder = checkNotNull(clientBuilder, "clientBuilder");
    this.checkpointGenerator = checkNotNull(checkpointGenerator, "checkpointGenerator");
    this.source = source;
    this.backlogBytesCheckThreshold = backlogBytesCheckThreshold;
  }

  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", checkpointGenerator);

    Config config =
        new Config(spec.getStreamName(), spec.getConsumerArn(), spec.getInitialPosition());
    RecordsSink sink = new LogCountRecordsSink();
    streamConsumer = StreamConsumer.init(config, clientBuilder, sink);
    return streamConsumer.isRunning();
  }

  @Override
  public boolean advance() throws IOException {
    currentRecord = streamConsumer.nextRecord();
    return currentRecord.isPresent();
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    return currentRecord.get().getUniqueId();
  }

  @Override
  public KinesisRecord getCurrent() throws NoSuchElementException {
    return currentRecord.get();
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return currentRecord.get().getApproximateArrivalTimestamp();
  }

  @Override
  public void close() throws IOException {
    try {
      streamConsumer.initiateGracefulShutdown();
      streamConsumer.awaitTermination();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

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
