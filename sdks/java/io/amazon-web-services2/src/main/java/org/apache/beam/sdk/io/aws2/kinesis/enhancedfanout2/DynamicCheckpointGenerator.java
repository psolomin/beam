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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;

/**
 * Creates {@link KinesisReaderCheckpoint}, which spans over all shards in given stream. List of
 * shards is obtained dynamically on call to {@link #generate(ClientBuilder)}.
 */
class DynamicCheckpointGenerator implements CheckpointGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicCheckpointGenerator.class);
  private final String streamName;
  private final String consumerArn;
  private final StartingPoint startingPoint;

  DynamicCheckpointGenerator(String streamName, String consumerArn, StartingPoint startingPoint) {
    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.startingPoint = startingPoint;
  }

  @Override
  public KinesisReaderCheckpoint generate(ClientBuilder clientBuilder)
      throws TransientKinesisException {
    List<Shard> streamShards = ShardsListingUtils.listShardsAtPoint(streamName, startingPoint);

    LOG.info(
        "Creating a checkpoint with following shards {} at {}",
        streamShards,
        startingPoint.getTimestamp());
    return new KinesisReaderCheckpoint(
        streamShards.stream()
            .map(
                shard ->
                    new ShardCheckpoint(streamName, consumerArn, shard.shardId(), startingPoint))
            .collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return String.format("Checkpoint generator for %s: %s", streamName, startingPoint);
  }
}
