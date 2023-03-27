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

import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.createIOOptions;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisSourceTest {
  private PipelineOptions opts() {
    AwsOptions options = PipelineOptionsFactory.fromArgs().as(AwsOptions.class);
    options.setAwsRegion(Region.AP_EAST_1);
    return options;
  }

  @Test
  public void testCreateReaderOfCorrectType() throws Exception {
    KinesisIO.Read readSpec =
        KinesisIO.read()
            .withStreamName("stream-xxx")
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    KinesisIO.Read readSpecEFO =
        KinesisIO.read()
            .withStreamName("stream-xxx")
            .withConsumerArn("consumer-aaa")
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    KinesisReaderCheckpoint initCheckpoint = new KinesisReaderCheckpoint(ImmutableList.of());
    UnboundedSource.UnboundedReader<KinesisRecord> reader =
        new KinesisSource(readSpec, initCheckpoint).createReader(opts(), null);
    assertThat(reader).isInstanceOf(KinesisReader.class);
    UnboundedSource.UnboundedReader<KinesisRecord> efoReader =
        new KinesisSource(readSpecEFO, initCheckpoint).createReader(opts(), null);
    assertThat(efoReader).isInstanceOf(EFOKinesisReader.class);
  }

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
