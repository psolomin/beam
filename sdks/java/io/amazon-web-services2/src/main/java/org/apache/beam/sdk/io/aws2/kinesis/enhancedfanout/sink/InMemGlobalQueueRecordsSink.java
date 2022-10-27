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
import net.bytebuddy.utility.nullability.MaybeNull;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class InMemGlobalQueueRecordsSink implements RecordsSink {
  private static final Logger LOG = LoggerFactory.getLogger(InMemGlobalQueueRecordsSink.class);

  private static final int MAX_CAPACITY = 10_000;
  private static final long QUEUE_OFFER_TIMEOUT_MS = 10_000;
  private static final long QUEUE_POLL_TIMEOUT_MS = 1_000;
  private final BlockingQueue<Record> queue = new LinkedBlockingQueue<>(MAX_CAPACITY);

  @Override
  public void submit(
      String shardId, Optional<KinesisClientRecord> record, String continuationSequenceNumber) {
    try {
      Record r = new Record(shardId, record, continuationSequenceNumber);
      if (!queue.offer(r, QUEUE_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS))
        throw new RuntimeException(
            String.format(
                "Queue overloaded, " + "failed to push event from shard %s after %s ms",
                shardId, QUEUE_OFFER_TIMEOUT_MS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while submitting record {} from shard {}", record, shardId);
    }
  }

  @Override
  public void submit(
      String shardId, List<KinesisClientRecord> records, String continuationSequenceNumber) {
    if (records.isEmpty()) submit(shardId, Optional.absent(), continuationSequenceNumber);
    else records.forEach(r -> submit(shardId, Optional.of(r), continuationSequenceNumber));
  }

  @Override
  public long getTotalCnt() {
    return queue.size();
  }

  @Override
  @MaybeNull
  public Record fetch() {
    try {
      return queue.poll(QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while fetching record");
      return null;
    }
  }
}
