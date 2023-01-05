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
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.InMemGlobalQueueRecordsSink;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.RecordsSink;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisEnhancedFanOutReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisEnhancedFanOutReader.class);

  private final KinesisIO.Read spec;
  private final ClientBuilder clientBuilder;
  private final KinesisEnhancedFanOutSource source;
  private final CheckpointGenerator checkpointGenerator;

  private CustomOptional<KinesisRecord> currentRecord = CustomOptional.absent();
  private CustomOptional<StreamConsumer> streamConsumer = CustomOptional.absent();

  KinesisEnhancedFanOutReader(
      KinesisIO.Read spec,
      ClientBuilder clientBuilder,
      CheckpointGenerator checkpointGenerator,
      KinesisEnhancedFanOutSource source
  ) {
    this.spec = checkNotNull(spec, "spec");
    this.clientBuilder = checkNotNull(clientBuilder, "clientBuilder");
    this.checkpointGenerator = checkNotNull(checkpointGenerator, "checkpointGenerator");
    this.source = source;
  }

  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", checkpointGenerator);
    Config config = Config.fromIOSpec(spec);
    RecordsSink sink = new InMemGlobalQueueRecordsSink();
    List<ShardCheckpoint> checkpoints =
        ShardsListingUtils.initSubscribedShardsProgressInfo(config, clientBuilder);
    KinesisReaderCheckpoint initialCheckpoint = new KinesisReaderCheckpoint(checkpoints);
    streamConsumer = CustomOptional.of(StreamConsumer.init(config, clientBuilder, initialCheckpoint, sink));
    return streamConsumer.get().isRunning();
  }

  @Override
  public boolean advance() throws IOException {
    currentRecord = streamConsumer.get().nextRecord();
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
      streamConsumer.get().initiateGracefulShutdown();
      streamConsumer.get().awaitTermination();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Instant getWatermark() {
    return streamConsumer.get().getWatermark();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return streamConsumer.get().getCheckpointMark();
  }

  @Override
  public UnboundedSource<KinesisRecord, ?> getCurrentSource() {
    return source;
  }
}
