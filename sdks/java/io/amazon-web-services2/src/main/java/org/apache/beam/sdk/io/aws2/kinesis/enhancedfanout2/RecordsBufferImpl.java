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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordsBufferImpl implements RecordsBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(RecordsBufferImpl.class);

  private final int maxCapacity = 20_000;
  private final long pollTimeoutMs = 5_000L;
  private final RecordsBufferState state;
  private final BlockingQueue<Record> queue;

  RecordsBufferImpl(RecordsBufferState state) {
    this.state = state;
    this.queue = new LinkedBlockingQueue<>(maxCapacity);
  }

  @Override
  public boolean push(Record record) {
    return false;
  }

  @Override
  public CustomOptional<Record> fetchOne() {
    try {
      Record recordOrNull = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
      if (recordOrNull != null) {
        state.ackRecord(recordOrNull);
        return CustomOptional.of(recordOrNull);
      } else return CustomOptional.absent();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while fetching record");
      return CustomOptional.absent();
    }
  }

  @Override
  public Instant getWatermark() {
    return state.getWatermark();
  }

  @Override
  public KinesisReaderCheckpoint getCheckpointMark() {
    return state.getCheckpointMark();
  }
}
