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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.signals;

import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.ShardEvent;
import software.amazon.awssdk.services.kinesis.model.ChildShard;

public class ReShardSignal implements ShardSubscriberSignal {
  private final String senderId;
  private final List<ChildShard> childShards;

  ReShardSignal(String senderId, List<ChildShard> childShards) {
    this.senderId = senderId;
    this.childShards = childShards;
  }

  public static ReShardSignal fromShardEvent(String senderId, ShardEvent event) {
    return new ReShardSignal(senderId, event.getWrappedEvent().childShards());
  }

  public String getSenderId() {
    return senderId;
  }

  public List<ChildShard> getChildShards() {
    return childShards;
  }

  @Override
  public String toString() {
    return "ReShardSignal{" + "senderId='" + senderId + '\'' + ", childShards=" + childShards + '}';
  }
}
