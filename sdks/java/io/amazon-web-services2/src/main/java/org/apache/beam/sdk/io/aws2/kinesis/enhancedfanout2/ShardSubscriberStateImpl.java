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

public class ShardSubscriberStateImpl implements ShardSubscriberState {
  private final KinesisShardEventsSubscriber subscriber;
  private ShardCheckpoint shardCheckpoint;

  public ShardSubscriberStateImpl(
      KinesisShardEventsSubscriber subscriber, ShardCheckpoint initialCheckpoint) {
    this.subscriber = subscriber;
    this.shardCheckpoint = initialCheckpoint;
  }

  @Override
  public void requestRecords(long n) {
    subscriber.requestRecords(n);
  }

  @Override
  public void ackRecord(ExtendedKinesisRecord record) {
    shardCheckpoint = shardCheckpoint.moveAfter(record.getContinuationSequenceNumber());
  }

  @Override
  public ShardCheckpoint getCheckpoint() {
    return shardCheckpoint;
  }

  @Override
  public void cancel() {
    subscriber.cancel();
  }
}
