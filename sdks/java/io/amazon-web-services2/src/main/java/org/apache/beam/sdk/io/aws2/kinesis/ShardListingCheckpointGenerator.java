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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

class ShardListingCheckpointGenerator implements CheckpointGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(ShardListingCheckpointGenerator.class);
  private static final int shardListingTimeoutMs = 10_000;

  private final KinesisIO.Read spec;

  public ShardListingCheckpointGenerator(KinesisIO.Read spec) {
    this.spec = spec;
  }

  @Override
  public <ClientT> KinesisReaderCheckpoint generate(ClientT client)
      throws TransientKinesisException {
    StartingPoint startingPoint = spec.getInitialPosition();

    if (client instanceof SimplifiedKinesisClient) {
      SimplifiedKinesisClient kinesis = (SimplifiedKinesisClient) client;
      List<Shard> streamShards =
          kinesis.listShardsAtPoint(
              Preconditions.checkArgumentNotNull(spec.getStreamName()),
              Preconditions.checkArgumentNotNull(startingPoint));

      LOG.info("Creating a checkpoint with following shards {} at {}", streamShards, startingPoint);
      return new KinesisReaderCheckpoint(
          streamShards.stream()
              .map(
                  shard ->
                      new ShardCheckpoint(
                          Preconditions.checkArgumentNotNull(spec.getStreamName()),
                          shard.shardId(),
                          Preconditions.checkArgumentNotNull(startingPoint)))
              .collect(Collectors.toList()));
    } else if (client instanceof KinesisAsyncClient) {
      KinesisAsyncClient kinesis = (KinesisAsyncClient) client;
      List<ShardCheckpoint> streamShards = generateShardsCheckpoints(spec, kinesis);
      LOG.info("Creating a checkpoint with following shards {} at {}", streamShards, startingPoint);
      return new KinesisReaderCheckpoint(streamShards);
    } else {
      throw new IllegalStateException(String.format("Unknown type of client %s", client));
    }
  }

  private static List<ShardCheckpoint> generateShardsCheckpoints(
      KinesisIO.Read readSpec, KinesisAsyncClient kinesis) {
    ListShardsRequest listShardsRequest =
        ListShardsRequest.builder()
            .streamName(checkArgumentNotNull(readSpec.getStreamName()))
            .shardFilter(buildFilter(readSpec))
            .build();

    ListShardsResponse response = tryListingShards(listShardsRequest, kinesis);
    return response.shards().stream()
        .map(
            s ->
                new ShardCheckpoint(
                    checkArgumentNotNull(readSpec.getStreamName()),
                    s.shardId(),
                    checkArgumentNotNull(readSpec.getInitialPosition())))
        .collect(Collectors.toList());
  }

  private static ListShardsResponse tryListingShards(
      ListShardsRequest listShardsRequest, KinesisAsyncClient kinesis) {
    try {
      LOG.info("Starting ListShardsRequest {}", listShardsRequest);
      ListShardsResponse response =
          kinesis.listShards(listShardsRequest).get(shardListingTimeoutMs, TimeUnit.MILLISECONDS);
      LOG.info("Shards found = {}", response.shards());
      return response;
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Error listing shards {}", e.getMessage());
      throw new RuntimeException("Error listing shards. Stopping");
    } catch (Exception e) {
      LOG.error("Unexpected error {}", e.getMessage());
      throw new RuntimeException("Error listing shards. Stopping");
    }
  }

  /**
   * FIXME: This repeats {@link SimplifiedKinesisClient#buildShardFilterForStartingPoint(String,
   * StartingPoint)}. Merge 2 implementations into 1.
   */
  private static ShardFilter buildFilter(KinesisIO.Read readSpec) {
    StartingPoint sp = checkArgumentNotNull(readSpec.getInitialPosition());
    switch (checkArgumentNotNull(sp.getPosition())) {
      case LATEST:
        return ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
      case AT_TIMESTAMP:
        return ShardFilter.builder()
            .type(ShardFilterType.AT_TIMESTAMP)
            .timestamp(TimeUtil.toJava(checkArgumentNotNull(sp.getTimestamp())))
            .build();
      case TRIM_HORIZON:
        return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
      default:
        throw new IllegalStateException(String.format("Invalid config %s", readSpec));
    }
  }
}
