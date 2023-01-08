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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.Checkers.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;

import java.io.Serializable;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.TimeUtil;
import org.joda.time.Instant;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Checkpoint mark for single shard in the stream. Current position in the shard is determined by
 * either:
 *
 * <ul>
 *   <li>{@link #shardIteratorType} if it is equal to {@link ShardIteratorType#LATEST} or {@link
 *       ShardIteratorType#TRIM_HORIZON}
 *   <li>combination of {@link #continuationSequenceNumber} and {@link #subSequenceNumber} if {@link
 *       ShardIteratorType#AFTER_SEQUENCE_NUMBER} or {@link ShardIteratorType#AT_SEQUENCE_NUMBER}
 * </ul>
 *
 * This class is immutable.
 */
public class ShardCheckpoint implements Serializable {
  private static final long serialVersionUID = 0L;

  private final String streamName;
  private final String consumerArn;
  private final String shardId;
  private final ShardIteratorType shardIteratorType;

  private final CustomOptional<String> continuationSequenceNumber;
  private final CustomOptional<Long> subSequenceNumber;
  private final CustomOptional<Instant> timestamp;

  public ShardCheckpoint(
      String streamName, String consumerArn, String shardId, StartingPoint startingPoint) {
    this(
        streamName,
        consumerArn,
        shardId,
        ShardIteratorType.fromValue(startingPoint.getPositionName()),
        CustomOptional.absent(),
        CustomOptional.absent(),
        startingPoint.getTimestamp() != null
            ? CustomOptional.of(startingPoint.getTimestamp())
            : CustomOptional.absent());
  }

  private ShardCheckpoint(
      String streamName,
      String consumerArn,
      String shardId,
      ShardIteratorType shardIteratorType,
      String continuationSequenceNumber,
      Long subSequenceNumber) {
    this(
        streamName,
        consumerArn,
        shardId,
        shardIteratorType,
        CustomOptional.of(continuationSequenceNumber),
        CustomOptional.of(subSequenceNumber),
        CustomOptional.absent());
  }

  private ShardCheckpoint(
      String streamName,
      String consumerArn,
      String shardId,
      ShardIteratorType shardIteratorType,
      CustomOptional<String> continuationSequenceNumber,
      CustomOptional<Long> subSequenceNumber,
      CustomOptional<Instant> timestamp) {
    this.shardIteratorType = checkNotNull(shardIteratorType, "shardIteratorType");
    this.streamName = checkNotNull(streamName, "streamName");
    this.consumerArn = checkNotNull(consumerArn, "consumerArn");
    this.shardId = checkNotNull(shardId, "shardId");

    if (shardIteratorType == AT_SEQUENCE_NUMBER || shardIteratorType == AFTER_SEQUENCE_NUMBER) {
      checkNotNull(
          continuationSequenceNumber,
          "You must provide sequence number for AT_SEQUENCE_NUMBER" + " or AFTER_SEQUENCE_NUMBER");
    } else {
      checkArgument(
          !continuationSequenceNumber.isPresent(),
          "Sequence number must be empty for LATEST, TRIM_HORIZON or AT_TIMESTAMP");
    }
    if (shardIteratorType == AT_TIMESTAMP) {
      checkArgument(!timestamp.isPresent(), "You must provide timestamp for AT_TIMESTAMP");
    } else {
      checkArgument(
          !timestamp.isPresent(),
          "Timestamp must be empty for an iterator type other than AT_TIMESTAMP");
    }

    this.subSequenceNumber = subSequenceNumber;
    this.continuationSequenceNumber = continuationSequenceNumber;
    this.timestamp = timestamp;
  }

  /**
   * Used to compare {@link ShardCheckpoint} object to {@link KinesisRecord}. Depending on the
   * underlying shardIteratorType, it will either compare the timestamp or the {@link
   * ExtendedSequenceNumber}.
   *
   * @param other
   * @return if current checkpoint mark points before or at given {@link ExtendedSequenceNumber}
   */
  public boolean isBeforeOrAt(KinesisRecord other, String continuationSequenceNumber) {
    if (shardIteratorType == AT_TIMESTAMP) {
      return timestamp.get().compareTo(other.getApproximateArrivalTimestamp()) <= 0;
    }
    int result = extendedSequenceNumber().compareTo(other.getExtendedSequenceNumber());
    if (result == 0) {
      return shardIteratorType == AT_SEQUENCE_NUMBER;
    }
    return result < 0;
  }

  private ExtendedSequenceNumber extendedSequenceNumber() {
    String fullSequenceNumber = continuationSequenceNumber.get();
    if (fullSequenceNumber == null) {
      fullSequenceNumber = shardIteratorType.toString();
    }
    return new ExtendedSequenceNumber(fullSequenceNumber, subSequenceNumber.get());
  }

  public StartingPosition toStartingPosition() {
    StartingPosition.Builder builder = StartingPosition.builder().type(shardIteratorType);
    switch (shardIteratorType) {
      case AT_TIMESTAMP:
        return builder.timestamp(TimeUtil.toJava(timestamp.get())).build();
      case AT_SEQUENCE_NUMBER:
      case AFTER_SEQUENCE_NUMBER:
        return builder.sequenceNumber(continuationSequenceNumber.get()).build();

      default:
        return builder.build();
    }
  }

  @Override
  public String toString() {
    return String.format(
        "Checkpoint %s for stream %s, consumer %s,  shard %s: %s",
        shardIteratorType, streamName, consumerArn, shardId, continuationSequenceNumber);
  }

  public ShardCheckpoint moveAfter(KinesisRecord record, String continuationSequenceNumber) {
    return new ShardCheckpoint(
        streamName,
        consumerArn,
        shardId,
        AFTER_SEQUENCE_NUMBER,
        continuationSequenceNumber,
        record.getSubSequenceNumber());
  }

  public ShardCheckpoint moveAfter(String continuationSequenceNumber) {
    return new ShardCheckpoint(
        streamName, consumerArn, shardId, AFTER_SEQUENCE_NUMBER, continuationSequenceNumber, 0L);
  }

  public String getStreamName() {
    return streamName;
  }

  public String getConsumerArn() {
    return consumerArn;
  }

  public String getShardId() {
    return shardId;
  }
}
