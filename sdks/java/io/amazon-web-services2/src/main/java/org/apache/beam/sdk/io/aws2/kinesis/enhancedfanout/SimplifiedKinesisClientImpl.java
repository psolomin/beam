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

import static org.apache.beam.sdk.io.aws2.kinesis.ErrorsUtils.wrapExceptions;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.TimeUtil;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

public class SimplifiedKinesisClientImpl implements SimplifiedKinesisClient {
  private static final long shardListingTimeoutMs = 15_000;

  private final KinesisAsyncClientProxy kinesis;

  SimplifiedKinesisClientImpl(KinesisAsyncClientProxy kinesis) {
    this.kinesis = kinesis;
  }

  @Override
  public List<Shard> listShardsAtPoint(String streamName, StartingPoint startingPoint)
      throws TransientKinesisException {
    ListShardsRequest listShardsRequest =
        ListShardsRequest.builder()
            .streamName(streamName)
            .shardFilter(buildFilter(startingPoint))
            .build();

    return tryListingShards(listShardsRequest).shards();
  }

  private static ShardFilter buildFilter(StartingPoint startingPoint) {
    switch (startingPoint.getPosition()) {
      case LATEST:
        return ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
      case AT_TIMESTAMP:
        return ShardFilter.builder()
            .type(ShardFilterType.AT_TIMESTAMP)
            .timestamp(TimeUtil.toJava(startingPoint.getTimestamp()))
            .build();
      case TRIM_HORIZON:
        return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
      default:
        throw new IllegalStateException(String.format("Invalid starting point %s", startingPoint));
    }
  }

  private ListShardsResponse tryListingShards(ListShardsRequest listShardsRequest)
      throws TransientKinesisException {
    return wrapExceptions(
        () ->
            kinesis
                .listShards(listShardsRequest)
                .get(shardListingTimeoutMs, TimeUnit.MILLISECONDS));
  }
}
