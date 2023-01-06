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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient.Builder;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class AsyncClientProxyImpl implements AsyncClientProxy {
  private final KinesisAsyncClient client;

  public AsyncClientProxyImpl() {
    RetryPolicy retryPolicy =
        RetryPolicy.builder()
            .backoffStrategy(BackoffStrategy.defaultStrategy())
            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
            .retryCondition(RetryCondition.defaultRetryCondition())
            .numRetries(10)
            .build();

    ClientOverrideConfiguration conf =
        ClientOverrideConfiguration.builder()
            .retryPolicy(retryPolicy)
            .apiCallTimeout(Duration.ofMinutes(6)) // must be > 5 minutes
            .build();

    Builder<NettyNioAsyncHttpClient.Builder> customHttpBuilder =
        NettyNioAsyncHttpClient.builder()
            .connectionMaxIdleTime(Duration.ofSeconds(180))
            .connectionAcquisitionTimeout(Duration.ofSeconds(60))
            .connectionTimeout(Duration.ofSeconds(120))
            .maxConcurrency(20)
            .maxPendingConnectionAcquires(120);

    client =
        KinesisAsyncClient.builder()
            .httpClientBuilder(customHttpBuilder)
            .overrideConfiguration(conf)
            .build();
  }

  @Override
  public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest request) {
    return client.listShards(request);
  }

  @Override
  public CompletableFuture<Void> subscribeToShard(
      SubscribeToShardRequest request, SubscribeToShardResponseHandler handler) {
    return client.subscribeToShard(request, handler);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
