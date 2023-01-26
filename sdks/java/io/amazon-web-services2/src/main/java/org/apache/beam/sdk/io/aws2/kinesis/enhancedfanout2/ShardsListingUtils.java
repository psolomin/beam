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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

public class ShardsListingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ShardsListingUtils.class);
  private static final int shardListingTimeoutMs = 10_000;

  static List<String> getListOfShards(Config config, AsyncClientProxy kinesis) {
    ListShardsRequest listShardsRequest =
        ListShardsRequest.builder()
            .streamName(config.getStreamName())
            .shardFilter(buildFilter(config))
            .build();

    ListShardsResponse response = tryListingShards(listShardsRequest, kinesis);
    return response.shards().stream().map(Shard::shardId).collect(Collectors.toList());
  }

  private static ListShardsResponse tryListingShards(
      ListShardsRequest listShardsRequest, AsyncClientProxy kinesis) {
    try {
      ListShardsResponse response =
          kinesis.listShards(listShardsRequest).get(shardListingTimeoutMs, TimeUnit.MILLISECONDS);
      LOG.debug("Shards found = {}", response.shards());
      return response;
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Error listing shards {}", e.getMessage());
      throw new RuntimeException("Error listing shards. Stopping");
    } catch (Exception e) {
      LOG.error("Unexpected error {}", e.getMessage());
      throw new RuntimeException("Error listing shards. Stopping");
    }
  }

  private static ShardFilter buildFilter(Config config) {
    switch (config.getStartingPoint().getPosition()) {
      case LATEST:
        return ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
      case AT_TIMESTAMP:
        return ShardFilter.builder()
            .type(ShardFilterType.AT_TIMESTAMP)
            .timestamp(config.getStartTimestamp())
            .build();
      case TRIM_HORIZON:
        return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
      default:
        throw new IllegalStateException(String.format("Invalid config %s", config));
    }
  }
}
