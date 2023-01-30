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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Config;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import software.amazon.kinesis.common.InitialPositionInStream;

public class Helpers {
  public static KinesisIO.Read createReadSpec() {
    return KinesisIO.read()
        .withStreamName("stream-01")
        .withConsumerArn("consumer-01")
        .withInitialPositionInStream(InitialPositionInStream.LATEST);
  }

  public static Config createConfig() {
    return new Config(
        "stream-01",
        "consumer-01",
        new StartingPoint(InitialPositionInStream.LATEST),
        Optional.absent(),
        1_000L,
            0L
    );
  }
}
