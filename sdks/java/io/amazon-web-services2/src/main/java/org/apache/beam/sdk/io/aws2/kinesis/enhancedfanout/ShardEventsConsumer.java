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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.RecordsSink;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class ShardEventsConsumer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ShardEventsConsumer.class);
  private AtomicBoolean isRunning;
  private final String shardId;
  private final ShardSubscriber shardSubscriber;
  private final ShardProgress shardProgress;
  private final RecordsSink recordsSink;
  private final ShardEventsConsumerState state;

  ShardEventsConsumer(
      StreamConsumer streamConsumer,
      Config config,
      ClientBuilder builder,
      RecordsSink recordsSink,
      String shardId,
      ShardProgress shardProgress,
      ShardEventsConsumerState state) {
    this.shardId = shardId;
    this.shardSubscriber =
        new ShardSubscriber(
            streamConsumer,
            builder.build(),
            config.getStreamName(),
            config.getConsumerArn(),
            shardId);
    this.shardProgress = shardProgress;
    this.recordsSink = recordsSink;
    this.state = state;
    this.isRunning = new AtomicBoolean(true);
  }

  static ShardEventsConsumer fromShardProgress(
      StreamConsumer streamConsumer,
      Config config,
      ClientBuilder builder,
      RecordsSink recordsSink,
      ShardProgress progress,
      ShardEventsConsumerState state) {
    return new ShardEventsConsumer(
        streamConsumer, config, builder, recordsSink, progress.getShardId(), progress, state);
  }

  @Override
  public void run() {
    while (isRunning.get()) {
      try {
        StartingPosition startingPosition = shardProgress.computeNextStartingPosition();
        LOG.info("Shard {} - Starting subscription with position = {}", shardId, startingPosition);
        boolean reSubscribe = shardSubscriber.subscribe(startingPosition, this::consume);
        if (!reSubscribe) {
          isRunning.set(false);
        } else {
          LOG.info("Will re-subscribe");
        }

      } catch (InterruptedException e) {
        LOG.warn("Interrupted while subscribing");
      }
    }
  }

  private void consume(SubscribeToShardEvent event) {
    long recordsArrivedInBatch = 0L;
    if (!event.records().isEmpty()) {
      List<KinesisClientRecord> clientRecords =
          new AggregatorUtil()
              .deaggregate(
                  event.records().stream()
                      .map(KinesisClientRecord::fromRecord)
                      .collect(Collectors.toList()));
      processClientRecords(clientRecords);
      recordsArrivedInBatch = event.records().size();
    }

    shardProgress.setLastSequenceNumber(event.continuationSequenceNumber(), recordsArrivedInBatch);
  }

  private void processClientRecords(List<KinesisClientRecord> clientRecords) {
    recordsSink.submit(shardId, clientRecords);
  }

  void initiateGracefulShutdown() {
    isRunning.set(false);
    shardSubscriber.cancel();
  }

  public static Instant getShardWatermark(ShardEventsConsumer shardEventsConsumer) {
    return shardEventsConsumer.state.getShardWatermark();
  }
}
