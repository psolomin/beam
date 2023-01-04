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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class InMemGlobalQueueRecordsSink implements RecordsSink {
  private static final Logger LOG = LoggerFactory.getLogger(InMemGlobalQueueRecordsSink.class);

  private final InMemGlobalSinkConfig config;
  private final BlockingQueue<Record> queue;

  public InMemGlobalQueueRecordsSink(InMemGlobalSinkConfig config) {
    this.config = config;
    this.queue = new LinkedBlockingQueue<>(config.getMaxCapacity());
  }

  public InMemGlobalQueueRecordsSink() {
    this.config = InMemGlobalSinkConfig.defaultConfig();
    this.queue = new LinkedBlockingQueue<>(config.getMaxCapacity());
  }

  @Override
  public void submit(
      String shardId, Optional<KinesisClientRecord> record, String continuationSequenceNumber) {
    try {
      Record r = new Record(shardId, record, continuationSequenceNumber);
      if (!queue.offer(r, config.getQueueOfferTimeoutMs(), TimeUnit.MILLISECONDS))
        throw new RuntimeException(
            String.format(
                "Queue overloaded, " + "failed to push event from shard %s after %s ms",
                shardId, config.getQueueOfferTimeoutMs()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while submitting record {} from shard {}", record, shardId);
    }
  }

  @Override
  public void submitMany(
      String shardId, List<KinesisClientRecord> records, String continuationSequenceNumber) {
    if (records.isEmpty()) submit(shardId, Optional.absent(), continuationSequenceNumber);
    else records.forEach(r -> submit(shardId, Optional.of(r), continuationSequenceNumber));
  }

  @Override
  public long getTotalCnt() {
    return queue.size();
  }

  @Override
  public Optional<Record> fetch() {
    try {
      Record recordOrNull = queue.poll(config.getQueuePollTimeoutMs(), TimeUnit.MILLISECONDS);
      if (recordOrNull != null) return Optional.of(recordOrNull);
      else return Optional.absent();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while fetching record");
      return Optional.absent();
    }
  }
}
