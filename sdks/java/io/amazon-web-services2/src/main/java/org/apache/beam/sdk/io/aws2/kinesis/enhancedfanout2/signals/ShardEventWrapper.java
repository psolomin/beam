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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.signals;

import java.util.Optional;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

public class ShardEventWrapper {
  private final ShardEventType type;
  private final Optional<SubscribeToShardEvent> event;
  private final Optional<Throwable> err;

  private ShardEventWrapper(
      ShardEventType type, Optional<SubscribeToShardEvent> event, Optional<Throwable> err) {
    this.type = type;
    this.event = event;
    this.err = err;
  }

  public static ShardEventWrapper fromNext(SubscribeToShardEvent event) {
    if (isReShard(event)) {
      return new ShardEventWrapper(ShardEventType.RE_SHARD, Optional.of(event), Optional.empty());
    } else if (isEventWithRecords(event)) {
      return new ShardEventWrapper(ShardEventType.RECORDS, Optional.of(event), Optional.empty());
    } else {
      throw new IllegalStateException(String.format("Unknown event type, no scenario: %s", event));
    }
  }

  public static ShardEventWrapper subscriptionComplete() {
    return new ShardEventWrapper(
        ShardEventType.SUBSCRIPTION_COMPLETE, Optional.empty(), Optional.empty());
  }

  public static ShardEventWrapper error(Throwable err) {
    return new ShardEventWrapper(ShardEventType.ERROR, Optional.empty(), Optional.of(err));
  }

  ShardEventType type() {
    return type;
  }

  public SubscribeToShardEvent getWrappedEvent() {
    if (type.equals(ShardEventType.RECORDS) && event.isPresent()) {
      return event.get();
    } else if (type.equals(ShardEventType.RE_SHARD) && event.isPresent()) {
      return event.get();
    } else {
      throw new IllegalStateException("Invalid");
    }
  }

  public Throwable getError() {
    if (type.equals(ShardEventType.ERROR) && err.isPresent()) {
      return err.get();
    } else {
      throw new IllegalStateException("Invalid");
    }
  }

  @Override
  public String toString() {
    return "ShardEvent{" + "type=" + type + ", event=" + event + ", err=" + err + '}';
  }

  private static boolean isReShard(SubscribeToShardEvent event) {
    return event.continuationSequenceNumber() == null && event.hasChildShards();
  }

  private static boolean isEventWithRecords(SubscribeToShardEvent event) {
    return event.continuationSequenceNumber() != null && event.hasRecords();
  }
}
