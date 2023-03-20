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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

class StaticCheckpointGenerator implements CheckpointGenerator {
  /**
   * Extracted from org.apache.beam:beam-sdks-java-io-amazon-web-services2:2.46.0.
   *
   * <pre>{@code
   * serialver -classpath "..<dependencies dir>/*" \
   *     org.apache.beam.sdk.io.aws2.kinesis.StaticCheckpointGenerator
   * }</pre>
   */
  private static final long serialVersionUID = 5972850685627641931L;

  private final KinesisReaderCheckpoint checkpoint;

  public StaticCheckpointGenerator(KinesisReaderCheckpoint checkpoint) {
    checkNotNull(checkpoint);
    this.checkpoint = checkpoint;
  }

  @Override
  public <ClientT> KinesisReaderCheckpoint generate(ClientT client) {
    return checkpoint;
  }

  @Override
  public String toString() {
    return checkpoint.toString();
  }
}
