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
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import software.amazon.kinesis.common.InitialPositionInStream;

public class Config {
  private final String streamName;
  private final String consumerArn;
  private final StartingPoint startingPoint;
  private final Optional<Instant> startTimestamp;

  public Config(
      String streamName,
      String consumerArn,
      StartingPoint startingPoint,
      Optional<Instant> startTimestamp) {
    if (startingPoint.getPosition().equals(InitialPositionInStream.AT_TIMESTAMP)
        && !startTimestamp.isPresent())
      throw new IllegalStateException("Timestamp must not be empty");

    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.startingPoint = startingPoint;
    this.startTimestamp = startTimestamp;
  }

  public Config(String streamName, String consumerArn, StartingPoint startingPoint) {
    this(streamName, consumerArn, startingPoint, Optional.empty());
  }

  public String getStreamName() {
    return streamName;
  }

  public String getConsumerArn() {
    return consumerArn;
  }

  public StartingPoint getStartingPoint() {
    return startingPoint;
  }

  public Instant getStartTimestamp() {
    return startTimestamp.get();
  }
}
