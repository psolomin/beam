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

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ShardSubscriberImpl implements ShardSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscriberImpl.class);
  private final String shardId;
  private final Config config;
  private final AtomicBoolean isRunning;

  ShardSubscriberImpl(Config config, String shardId) {
    this.config = config;
    this.shardId = shardId;
    this.isRunning = new AtomicBoolean(true);
  }

  @Override
  public void run() {
    while (isRunning.get()) {
      try {
        Thread.sleep(1_000);
      } catch (InterruptedException e) {
        LOG.warn(
            "Interrupted in subscription loop for stream {} shard {}",
            config.getStreamName(),
            shardId);
      }
    }
  }

  @Override
  public boolean stop() {
    isRunning.set(false);
    return true;
  }
}
