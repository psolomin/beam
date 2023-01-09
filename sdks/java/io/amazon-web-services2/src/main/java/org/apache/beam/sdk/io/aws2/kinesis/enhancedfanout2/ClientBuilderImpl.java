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

import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;

class ClientBuilderImpl implements ClientBuilder {
  private final ClientBuilderFactory builderFactory;
  private final KinesisIO.Read readSpec;

  ClientBuilderImpl(ClientBuilderFactory builderFactory, KinesisIO.Read readSpec) {
    this.builderFactory = builderFactory;
    this.readSpec = readSpec;
  }

  @Override
  public AsyncClientProxy build() {
    return new AsyncClientProxyImpl(builderFactory, readSpec);
  }
}
