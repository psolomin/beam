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

public class InMemGlobalSinkConfig {
  private static final int DEFAULT_MAX_CAPACITY = 10_000;
  private static final long DEFAULT_QUEUE_OFFER_TIMEOUT_MS = 10_000;
  private static final long DEFAULT_QUEUE_POLL_TIMEOUT_MS = 1_000;
  private static final long DEFAULT_QUEUE_EMPTY_TIMEOUT_MS = 60_000;

  private final int maxCapacity;
  private final long queueOfferTimeoutMs;
  private final long queuePollTimeoutMs;
  private final long queueEmptyTimeoutMs;

  public InMemGlobalSinkConfig(
      int maxCapacity,
      long queueOfferTimeoutMs,
      long queuePollTimeoutMs,
      long queueEmptyTimeoutMs) {
    this.maxCapacity = maxCapacity;
    this.queueOfferTimeoutMs = queueOfferTimeoutMs;
    this.queuePollTimeoutMs = queuePollTimeoutMs;
    this.queueEmptyTimeoutMs = queueEmptyTimeoutMs;
  }

  public static InMemGlobalSinkConfig defaultConfig() {
    return new InMemGlobalSinkConfig(
        DEFAULT_MAX_CAPACITY,
        DEFAULT_QUEUE_OFFER_TIMEOUT_MS,
        DEFAULT_QUEUE_POLL_TIMEOUT_MS,
        DEFAULT_QUEUE_EMPTY_TIMEOUT_MS);
  }

  public int getMaxCapacity() {
    return maxCapacity;
  }

  public long getQueueOfferTimeoutMs() {
    return queueOfferTimeoutMs;
  }

  public long getQueuePollTimeoutMs() {
    return queuePollTimeoutMs;
  }

  public long getQueueEmptyTimeoutMs() {
    return queueEmptyTimeoutMs;
  }
}
