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

import java.time.Instant;
import java.util.Optional;

public class Config {
  private final String streamName;
  private final String consumerArn;
  private final StartType startType;
  private final Optional<Instant> startTimestamp;

  public Config(
      String streamName,
      String consumerArn,
      StartType startType,
      Optional<Instant> startTimestamp) {
    if (startType.equals(StartType.AT_TIMESTAMP) && startTimestamp.isEmpty())
      throw new IllegalStateException("Timestamp must not be empty");

    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.startType = startType;
    this.startTimestamp = startTimestamp;
  }

  public Config(String streamName, String consumerArn, StartType startType) {
    this(streamName, consumerArn, startType, Optional.empty());
  }

  public String getStreamName() {
    return streamName;
  }

  public String getConsumerArn() {
    return consumerArn;
  }

  public StartType getStartType() {
    return startType;
  }

  public Instant getStartTimestamp() {
    return startTimestamp.get();
  }
}
