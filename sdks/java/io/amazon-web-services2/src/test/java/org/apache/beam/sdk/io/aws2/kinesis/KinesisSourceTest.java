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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.createIOOptions;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class KinesisSourceTest {
  @Test
  public void testConsumerArnNotPassed() {
    KinesisIO.Read readSpec = KinesisIO.read().withStreamName("stream-xxx");
    KinesisIOOptions options = createIOOptions();
    assertThat(KinesisSource.SourceResolver.resolveConsumerArn(readSpec, options)).isNull();
  }

  @Test
  public void testConsumerArnPassedInIO() {
    KinesisIO.Read readSpec =
        KinesisIO.read().withStreamName("stream-xxx").withConsumerArn("arn::consumer-yyy");

    KinesisIOOptions options = createIOOptions();
    assertThat(KinesisSource.SourceResolver.resolveConsumerArn(readSpec, options))
        .isEqualTo("arn::consumer-yyy");
  }

  @Test
  public void testConsumerArnPassedInPipelineOptions() {
    KinesisIO.Read readSpec = KinesisIO.read().withStreamName("stream-xxx");

    KinesisIOOptions options =
        createIOOptions("--kinesisIOConsumerArns={\"stream-xxx\": \"arn-01\"}");
    assertThat(KinesisSource.SourceResolver.resolveConsumerArn(readSpec, options))
        .isEqualTo("arn-01");
  }

  @Test
  public void testConsumerArnForSpecificStreamNotPassedInPipelineOptions() {
    KinesisIO.Read readSpec = KinesisIO.read().withStreamName("stream-xxx");
    KinesisIOOptions options =
        createIOOptions("--kinesisIOConsumerArns={\"stream-01\": \"arn-01\"}");
    assertThat(KinesisSource.SourceResolver.resolveConsumerArn(readSpec, options)).isNull();
  }

  @Test
  public void testConsumerArnInPipelineOptionsOverwritesIOSetting() {
    KinesisIO.Read readSpec =
        KinesisIO.read().withStreamName("stream-xxx").withConsumerArn("arn-ignored");

    KinesisIOOptions options =
        createIOOptions("--kinesisIOConsumerArns={\"stream-xxx\": \"arn-01\"}");
    assertThat(KinesisSource.SourceResolver.resolveConsumerArn(readSpec, options))
        .isEqualTo("arn-01");
  }

  @Test
  public void testConsumerArnInDiscardsIOSetting() {
    KinesisIO.Read readSpec =
        KinesisIO.read().withStreamName("stream-xxx").withConsumerArn("arn-ignored");

    KinesisIOOptions options = createIOOptions("--kinesisIOConsumerArns={\"stream-xxx\": null}");
    assertThat(KinesisSource.SourceResolver.resolveConsumerArn(readSpec, options)).isNull();
  }
}
