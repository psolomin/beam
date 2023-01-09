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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2.helpers.Helpers.createReadSpec;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.junit.Test;

public class KinesisEnhancesFanOutSourceTest {
  @Test
  public void ensureSerializable() {
    KinesisIO.Read readSpec = createReadSpec();
    KinesisEnhancedFanOutSource source =
        new KinesisEnhancedFanOutSource(readSpec, ClientBuilderFactory.defaultFactory());
    byte[] data = SerializationUtils.serialize(source);
    SerializationUtils.deserialize(data); // no error
  }
}
