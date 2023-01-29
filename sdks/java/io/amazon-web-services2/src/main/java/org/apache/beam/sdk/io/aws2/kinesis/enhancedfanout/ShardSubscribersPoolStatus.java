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

import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ShardSubscribersPoolStatus {
  private final boolean isOk;
  private final @Nullable String shardId;
  private final @Nullable Throwable err;

  private ShardSubscribersPoolStatus(
      boolean isOk, @Nullable String shardId, @Nullable Throwable err) {
    this.isOk = isOk;
    this.shardId = shardId;
    this.err = err;
  }

  public static ShardSubscribersPoolStatus okStatus() {
    return new ShardSubscribersPoolStatus(true, null, null);
  }

  public static ShardSubscribersPoolStatus errStatus(String shardId, Throwable e) {
    return new ShardSubscribersPoolStatus(false, shardId, e);
  }

  public boolean isOk() {
    return isOk;
  }

  public String getShardId() {
    return Checkers.checkNotNull(shardId, "shard id");
  }

  public Throwable getErr() {
    return Checkers.checkNotNull(err, "err");
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShardSubscribersPoolStatus that = (ShardSubscribersPoolStatus) o;
    return isOk == that.isOk;
  }

  @Override
  public int hashCode() {
    return Objects.hash(isOk);
  }
}
