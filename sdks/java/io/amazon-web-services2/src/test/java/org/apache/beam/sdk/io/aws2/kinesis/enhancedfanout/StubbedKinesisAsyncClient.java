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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import avro.shaded.com.google.common.base.Objects;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

@SuppressWarnings({"MissingOverride", "FutureReturnValueIgnored"})
class StubbedKinesisAsyncClient implements KinesisAsyncClient {

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private final int publisherRateMs;

  private final Map<String, Deque<StubbedSdkPublisher>> stubbedPublishers = new HashMap<>();
  private final List<String> initialShardsIds;

  private final ConcurrentLinkedQueue<ListShardsRequest> listShardsRequestsSeen =
      new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<SubscribeToShardRequest> subscribeRequestsSeen =
      new ConcurrentLinkedQueue<>();

  StubbedKinesisAsyncClient(int publisherRateMs, List<String> initialShardsIds) {
    this.publisherRateMs = publisherRateMs;
    this.initialShardsIds = initialShardsIds;
  }

  /**
   * Stubs a subscribeToShard call with the provided events, optionally terminating with an error or
   * otherwise normally as soon as all events are delivered.
   */
  public CanFail stubSubscribeToShard(String shardId, SubscribeToShardEventStream... events) {
    StubbedSdkPublisher publisher = new StubbedSdkPublisher(events);
    stubbedPublishers.computeIfAbsent(shardId, id -> new ArrayDeque<>()).add(publisher);
    return publisher;
  }

  @Override
  public CompletableFuture<Void> subscribeToShard(
      SubscribeToShardRequest req, SubscribeToShardResponseHandler resp) {
    subscribeRequestsSeen.add(req);
    Deque<StubbedSdkPublisher> publishers =
        checkNotNull(stubbedPublishers.get(req.shardId()), "Not stubbed");
    StubbedSdkPublisher publisher = Objects.firstNonNull(publishers.poll(), new NoopSdkPublisher());
    resp.onEventStream(publisher);
    return publisher.result;
  }

  @Override
  public void close() {
    scheduler.shutdown();
  }

  @Override
  public String serviceName() {
    return "kinesis";
  }

  @Override
  public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {
    listShardsRequestsSeen.add(listShardsRequest);
    return CompletableFuture.completedFuture(
        ListShardsResponse.builder().shards(buildShards()).build());
  }

  private List<Shard> buildShards() {
    return initialShardsIds.stream()
        .map(
            shardId ->
                Shard.builder()
                    .shardId(shardId)
                    .sequenceNumberRange(
                        SequenceNumberRange.builder().startingSequenceNumber("ignored").build())
                    .build())
        .collect(Collectors.toList());
  }

  public interface CanFail {
    void failWith(Throwable error);
  }

  // Never delivers any event and completes only when cancelled.
  private class NoopSdkPublisher extends StubbedSdkPublisher {
    NoopSdkPublisher() {
      super(new SubscribeToShardEventStream[] {});
    }

    @Override
    public void subscribe(Subscriber<? super SubscribeToShardEventStream> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {
              result.complete(null);
            }
          });
    }
  }

  private class StubbedSdkPublisher implements SdkPublisher<SubscribeToShardEventStream>, CanFail {
    final CompletableFuture<Void> result = new CompletableFuture<>();
    final SubscribeToShardEventStream[] events;
    @Nullable Throwable error = null;

    StubbedSdkPublisher(SubscribeToShardEventStream[] events) {
      this.events = events;
    }

    public void failWith(Throwable error) {
      this.error = error;
    }

    @Override
    public void subscribe(Subscriber<? super SubscribeToShardEventStream> subscriber) {
      AtomicInteger requested = new AtomicInteger();
      subscriber.onSubscribe(
          new Subscription() {
            {
              scheduler.schedule(this::publish, publisherRateMs, MILLISECONDS);
            }

            @Override
            public void request(long n) {
              requested.incrementAndGet();
            }

            @Override
            public void cancel() {
              result.complete(null);
            }

            int idx = 0;

            void publish() {
              if (!result.isDone() && idx < events.length) {
                if (requested.getAndUpdate(i -> Math.max(0, i - 1)) > 0) {
                  subscriber.onNext(events[idx++]);
                }
                scheduler.schedule(this::publish, publisherRateMs, MILLISECONDS);
              } else if (error != null) {
                subscriber.onError(error);
                result.completeExceptionally(error);
              } else {
                subscriber.onComplete();
                result.complete(null);
              }
            }
          });
    }
  }

  public List<SubscribeToShardRequest> subscribeRequestsSeen() {
    return new ArrayList<>(subscribeRequestsSeen);
  }

  public List<ListShardsRequest> listRequestsSeen() {
    return new ArrayList<>(listShardsRequestsSeen);
  }
}
