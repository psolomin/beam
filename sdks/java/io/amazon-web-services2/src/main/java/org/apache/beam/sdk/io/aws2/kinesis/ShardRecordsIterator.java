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

import static org.apache.beam.sdk.io.aws2.kinesis.ErrorsUtils.wrapExceptions;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * Iterates over records in a single shard. Records are retrieved in batches via calls to {@link
 * ShardRecordsIterator#readNextBatch()}. Client has to confirm processed records by calling {@link
 * ShardRecordsIterator#ackRecord(KinesisRecord)} method.
 */
class ShardRecordsIterator {

  private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

  private final SimplifiedKinesisClient kinesis;
  private final RecordFilter filter;
  private final String streamName;
  private final Optional<String> consumerArn;
  private final String shardId;
  private final AtomicReference<ShardCheckpoint> checkpoint;
  private final WatermarkPolicy watermarkPolicy;
  private final WatermarkPolicyFactory watermarkPolicyFactory;
  private final WatermarkPolicy latestRecordTimestampPolicy =
      WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();
  private String shardIterator;
  private AtomicBoolean resubscribe = new AtomicBoolean(true);
  private AtomicLong millisBehindLatest = new AtomicLong(Long.MAX_VALUE);

  ShardRecordsIterator(
      ShardCheckpoint initialCheckpoint,
      SimplifiedKinesisClient simplifiedKinesisClient,
      WatermarkPolicyFactory watermarkPolicyFactory)
      throws TransientKinesisException {
    this(initialCheckpoint, simplifiedKinesisClient, watermarkPolicyFactory, new RecordFilter());
  }

  ShardRecordsIterator(
      ShardCheckpoint initialCheckpoint,
      SimplifiedKinesisClient simplifiedKinesisClient,
      WatermarkPolicyFactory watermarkPolicyFactory,
      RecordFilter filter)
      throws TransientKinesisException {
    this.checkpoint = new AtomicReference<>(checkNotNull(initialCheckpoint, "initialCheckpoint"));
    this.filter = checkNotNull(filter, "filter");
    this.kinesis = checkNotNull(simplifiedKinesisClient, "simplifiedKinesisClient");
    this.streamName = initialCheckpoint.getStreamName();
    this.consumerArn = initialCheckpoint.getConsumerArn();
    this.shardId = initialCheckpoint.getShardId();
    this.shardIterator = initialCheckpoint.getShardIterator(kinesis);
    this.watermarkPolicy = watermarkPolicyFactory.createWatermarkPolicy();
    this.watermarkPolicyFactory = watermarkPolicyFactory;
  }

  List<KinesisRecord> readNextBatch()
      throws TransientKinesisException, KinesisShardClosedException {
    if (shardIterator == null) {
      throw new KinesisShardClosedException(
          String.format(
              "Shard iterator reached end of the shard: streamName=%s, shardId=%s",
              streamName, shardId));
    }
    GetKinesisRecordsResult response = fetchRecords();
    LOG.debug(
        "Fetched {} new records from shard: streamName={}, shardId={}",
        response.getRecords().size(),
        streamName,
        shardId);

    List<KinesisRecord> filteredRecords = filter.apply(response.getRecords(), checkpoint.get());
    return filteredRecords;
  }

  boolean hasConsumer() {
    return consumerArn.isPresent();
  }

  String getConsumerInfo() {
    return consumerArn.or("N/A");
  }

  void subscribeToShard(Consumer<KinesisRecord> consumer) throws TransientKinesisException {
    wrapExceptions(
        () ->
            checkpoint
                .get()
                .subscribeToShard(
                    resubscribe,
                    kinesis,
                    createVisitor(consumer),
                    e -> LOG.error("Error during stream - " + e.getMessage()))
                .join());
  }

  private GetKinesisRecordsResult fetchRecords() throws TransientKinesisException {
    try {
      GetKinesisRecordsResult response = kinesis.getRecords(shardIterator, streamName, shardId);
      shardIterator = response.getNextShardIterator();
      return response;
    } catch (ExpiredIteratorException e) {
      LOG.info(
          "Refreshing expired iterator for shard: streamName={}, shardId={}",
          streamName,
          shardId,
          e);
      shardIterator = checkpoint.get().getShardIterator(kinesis);
      return fetchRecords();
    }
  }

  ShardCheckpoint getCheckpoint() {
    return checkpoint.get();
  }

  void ackRecord(KinesisRecord record) {
    checkpoint.set(checkpoint.get().moveAfter(record));
    watermarkPolicy.update(record);
    latestRecordTimestampPolicy.update(record);
    resubscribe.set(true);
  }

  Instant getShardWatermark() {
    return watermarkPolicy.getWatermark();
  }

  Instant getLatestRecordTimestamp() {
    return latestRecordTimestampPolicy.getWatermark();
  }

  String getShardId() {
    return shardId;
  }

  String getStreamName() {
    return streamName;
  }

  List<ShardRecordsIterator> findSuccessiveShardRecordIterators() throws TransientKinesisException {
    List<Shard> shards = kinesis.listShardsFollowingClosedShard(streamName, shardId);
    List<ShardRecordsIterator> successiveShardRecordIterators = new ArrayList<>();
    for (Shard shard : shards) {
      if (shardId.equals(shard.parentShardId())) {
        ShardCheckpoint shardCheckpoint =
            new ShardCheckpoint(
                streamName,
                consumerArn,
                shard.shardId(),
                new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
        successiveShardRecordIterators.add(
            new ShardRecordsIterator(shardCheckpoint, kinesis, watermarkPolicyFactory));
      }
    }
    return successiveShardRecordIterators;
  }

  SubscribeToShardResponseHandler.Visitor createVisitor(Consumer<KinesisRecord> consumer) {
    return new SubscribeToShardResponseHandler.Visitor() {
      @Override
      public void visit(SubscribeToShardEvent event) {
        LOG.debug("Received subscribe to shard event {}", event);
        if (event.continuationSequenceNumber() == null) {
          LOG.info(
              "Shard {} is split into {}. Will not resubscribe.", shardId, event.childShards());
          resubscribe.set(false);
        }
        if (!event.records().isEmpty()) {
          millisBehindLatest.set(event.millisBehindLatest());
          List<KinesisRecord> kinesisRecords =
              SimplifiedKinesisClient.deaggregate(event.records()).stream()
                  .map(r -> new KinesisRecord(r, streamName, shardId))
                  .collect(Collectors.toList());
          filter.apply(kinesisRecords, checkpoint.get()).forEach(consumer);
        }
      }
    };
  }
}
