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

import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.buildClient;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.kinesis.common.KinesisClientUtil;

class KinesisSource extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {
  /**
   * Extracted from org.apache.beam:beam-sdks-java-io-amazon-web-services2:2.46.0.
   *
   * <pre>{@code
   * serialver -classpath "target/dependency/*" \
   *   org.apache.beam.sdk.io.aws2.kinesis.KinesisSource
   * }</pre>
   */
  private static final long serialVersionUID = -6683426380627845784L;

  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

  private final KinesisIO.Read spec;
  private final CheckpointGenerator checkpointGenerator;

  KinesisSource(KinesisIO.Read read) {
    this(read, new ShardListingCheckpointGenerator(read));
  }

  private KinesisSource(KinesisIO.Read spec, CheckpointGenerator initialCheckpoint) {
    this.spec = checkNotNull(spec);
    this.checkpointGenerator = checkNotNull(initialCheckpoint);
  }

  /**
   * FIXME: Avoid duplicated code.
   *
   * <p>Similar instance {@link ShardListingCheckpointGenerator#generate(Object)}.
   */
  @Override
  public List<KinesisSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
    List<KinesisSource> sources = newArrayList();
    KinesisReaderCheckpoint checkpoint = initCheckpoint(options);
    for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
      sources.add(new KinesisSource(spec, new StaticCheckpointGenerator(partition)));
    }
    return sources;
  }

  @Override
  public UnboundedReader<KinesisRecord> createReader(
      PipelineOptions options, @Nullable KinesisReaderCheckpoint checkpointMark)
      throws IOException {
    CheckpointGenerator checkpointGenerator = this.checkpointGenerator;
    if (checkpointMark != null) {
      checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
    }

    String consumerArn = resolveConsumerArn(spec, options);

    if (consumerArn == null) {
      LOG.info("Creating new reader using {}", checkpointGenerator);
      return new KinesisReader(spec, createClient(options), checkpointGenerator, this);
    } else {
      LOG.info("Creating new EFO reader using {}", checkpointGenerator);
      return new EFOKinesisReader(
          spec, consumerArn, createAsyncClient(options), checkpointGenerator, this);
    }
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }

  private SimplifiedKinesisClient createClient(PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    Supplier<KinesisClient> kinesisSupplier;
    Supplier<CloudWatchClient> cloudWatchSupplier;
    if (spec.getAWSClientsProvider() != null) {
      kinesisSupplier =
          Preconditions.checkArgumentNotNull(spec.getAWSClientsProvider())::getKinesisClient;
      cloudWatchSupplier =
          Preconditions.checkArgumentNotNull(spec.getAWSClientsProvider())::getCloudWatchClient;
    } else {
      ClientConfiguration config =
          Preconditions.checkArgumentNotNull(spec.getClientConfiguration());
      kinesisSupplier = () -> buildClient(awsOptions, KinesisClient.builder(), config);
      cloudWatchSupplier = () -> buildClient(awsOptions, CloudWatchClient.builder(), config);
    }
    return new SimplifiedKinesisClient(
        kinesisSupplier, cloudWatchSupplier, spec.getRequestRecordsLimit());
  }

  private KinesisAsyncClient createAsyncClient(PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    ClientBuilderFactory builderFactory = ClientBuilderFactory.getFactory(awsOptions);
    KinesisAsyncClientBuilder adjustedBuilder =
        KinesisClientUtil.adjustKinesisClientBuilder(KinesisAsyncClient.builder());
    return builderFactory
        .create(adjustedBuilder, checkArgumentNotNull(spec.getClientConfiguration()), awsOptions)
        .build();
  }

  /**
   * Provides final consumer ARN config.
   *
   * <p>{@link PipelineOptions} instance will overwrite anything given by {@link KinesisIO.Read}.
   */
  static @Nullable String resolveConsumerArn(KinesisIO.Read spec, PipelineOptions options) {
    String streamName = Preconditions.checkArgumentNotNull(spec.getStreamName());
    KinesisIOOptions sourceOptions = options.as(KinesisIOOptions.class);
    Map<String, String> streamToArnMapping = sourceOptions.getKinesisIOConsumerArns();

    String consumerArn;
    if (streamToArnMapping.containsKey(streamName)) {
      consumerArn = streamToArnMapping.get(streamName); // can resolve to null too
    } else {
      consumerArn = spec.getConsumerArn();
    }

    return consumerArn;
  }

  KinesisReaderCheckpoint initCheckpoint(PipelineOptions options) throws Exception {
    String consumerArn = resolveConsumerArn(spec, options);
    if (consumerArn == null) {
      try (SimplifiedKinesisClient client = createClient(options)) {
        return checkpointGenerator.generate(client);
      }
    } else {
      try (KinesisAsyncClient kinesis = createAsyncClient(options)) {
        return checkpointGenerator.generate(kinesis);
      }
    }
  }
}
