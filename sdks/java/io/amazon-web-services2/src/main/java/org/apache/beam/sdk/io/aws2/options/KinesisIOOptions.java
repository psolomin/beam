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
package org.apache.beam.sdk.io.aws2.options;

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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Allows passing modify-able options for {@link org.apache.beam.sdk.io.aws2.kinesis.KinesisIO}.
 *
 * <p>This class is not bound to source only and can have modifiable options for sink, too.
 *
 * <p>This class appeared during the implementation of EFO consumer. {@link
 * org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Read} is serialized with the entire Source object
 * (at least in Flink runner) which was a trouble for EFO feature design: if consumer ARN is part of
 * KinesisIO.Read object, when started from a Flink savepoint, consumer ARN string or null value
 * would be forced from the savepoint. Consequences of this are:
 *
 * <ol>
 *   <li>Once a Kinesis source is started, its consumer ARN can't be changed without loosing state
 *       (checkpoint-ed shard progress).
 *   <li>Kinesis source can not have seamless enabling / disabling of EFO feature without loosing
 *       state (checkpoint-ed shard progress).
 * </ol>
 *
 * <p>This {@link PipelineOptions} extension allows having modifiable configurations for {@link
 * org.apache.beam.sdk.io.UnboundedSource#split(int, PipelineOptions)} and {@link
 * org.apache.beam.sdk.io.UnboundedSource#createReader(PipelineOptions,
 * UnboundedSource.CheckpointMark)}, which is essential for seamless EFO switch on / off.
 */
@Experimental(Kind.SOURCE_SINK)
public interface KinesisIOOptions extends PipelineOptions {
  /**
   * {@link KinesisSourceToConsumerMapping} used to enable / disable EFO.
   *
   * <p>Example:
   *
   * <pre>{@code --kinesisSourceToConsumerMapping={
   *   "stream-01": "arn:aws:kinesis:...:stream/stream-01/consumer/consumer-01:1678576714",
   *   "stream-02": "arn:aws:kinesis:...:stream/stream-02/consumer/my-consumer:1679576982",
   *   ...
   * }}</pre>
   */
  @Description("Mapping of streams' names to consumer ARNs of those streams")
  @Default.InstanceFactory(
      KinesisSourceToConsumerMapping.KinesisSourceToConsumerMappingFactory.class)
  KinesisSourceToConsumerMapping getKinesisSourceToConsumerMapping();

  void setKinesisSourceToConsumerMapping(KinesisSourceToConsumerMapping value);
}
