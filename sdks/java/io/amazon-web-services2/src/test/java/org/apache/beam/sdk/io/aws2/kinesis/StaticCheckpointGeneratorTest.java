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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

public class StaticCheckpointGeneratorTest {
  /**
   * {@link KinesisSource} serialization changed after some changes in its data.
   *
   * <p>Incompatibility caused failures in the following scenario:
   *
   * <ol>
   *   <li>Create Flink savepoint with Beam 2.46.0
   *   <li>Start from Flink savepoint with Beam 2.47.0-SNAPSHOT in development branch
   * </ol>
   *
   * <p>Neither of {@link CheckpointGenerator} implementations had explicit serialVersionUID, which
   * caused errors:
   *
   * <p><small>Caused by: java.io.InvalidClassException:
   * org.apache.beam.sdk.io.aws2.kinesis.StaticCheckpointGenerator; local class incompatible: stream
   * classdesc serialVersionUID = 5972850685627641931, local class serialVersionUID =
   * -1716374792629517553</small>
   *
   * <p><small>Caused by: java.io.InvalidClassException:
   * org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint; local class incompatible: stream classdesc
   * serialVersionUID = 103536540299998471, local class serialVersionUID =
   * 2842489499429532931</small>
   */
  @Test
  public void testCompatibility() throws IOException, ClassNotFoundException {
    ShardCheckpoint shardCheckpoint =
        new ShardCheckpoint(
            "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "42", 12L);
    KinesisReaderCheckpoint checkpoint =
        new KinesisReaderCheckpoint(ImmutableList.of(shardCheckpoint));
    StaticCheckpointGenerator generator = new StaticCheckpointGenerator(checkpoint);
    String serializedGenerator = serializeObjectToString(generator);
    // This sequence generated with Beam release 2.46.0 and AdoptOpenJDK-11.0.11+9
    String serializedGeneratorRelease2460 =
        "rO0ABXNyAD1vcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3czIua2luZXNpcy5TdGF0aWNDaGVja3BvaW50R2VuZXJhdG9yUuPUDZVIsEsCAAFMAApjaGVja3BvaW50dAA9TG9yZy9hcGFjaGUvYmVhbS9zZGsvaW8vYXdzMi9raW5lc2lzL0tpbmVzaXNSZWFkZXJDaGVja3BvaW50O3hwc3IAO29yZy5hcGFjaGUuYmVhbS5zZGsuaW8uYXdzMi5raW5lc2lzLktpbmVzaXNSZWFkZXJDaGVja3BvaW50octvds7/pckCAAFMABBzaGFyZENoZWNrcG9pbnRzdAAQTGphdmEvdXRpbC9MaXN0O3hwc3IAXW9yZy5hcGFjaGUuYmVhbS52ZW5kb3IuZ3VhdmEudjI2XzBfanJlLmNvbS5nb29nbGUuY29tbW9uLmNvbGxlY3QuSW1tdXRhYmxlTGlzdCRTZXJpYWxpemVkRm9ybQAAAAAAAAAAAgABWwAIZWxlbWVudHN0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuT2JqZWN0O5DOWJ8QcylsAgAAeHAAAAABc3IAM29yZy5hcGFjaGUuYmVhbS5zZGsuaW8uYXdzMi5raW5lc2lzLlNoYXJkQ2hlY2twb2ludAFv1e9R2rUHAgAGTAAOc2VxdWVuY2VOdW1iZXJ0ABJMamF2YS9sYW5nL1N0cmluZztMAAdzaGFyZElkcQB+AAxMABFzaGFyZEl0ZXJhdG9yVHlwZXQAQUxzb2Z0d2FyZS9hbWF6b24vYXdzc2RrL3NlcnZpY2VzL2tpbmVzaXMvbW9kZWwvU2hhcmRJdGVyYXRvclR5cGU7TAAKc3RyZWFtTmFtZXEAfgAMTAARc3ViU2VxdWVuY2VOdW1iZXJ0ABBMamF2YS9sYW5nL0xvbmc7TAAJdGltZXN0YW1wdAAXTG9yZy9qb2RhL3RpbWUvSW5zdGFudDt4cHQAAjQydAAJc2hhcmQtMDAwfnIAP3NvZnR3YXJlLmFtYXpvbi5hd3NzZGsuc2VydmljZXMua2luZXNpcy5tb2RlbC5TaGFyZEl0ZXJhdG9yVHlwZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQAFUFGVEVSX1NFUVVFTkNFX05VTUJFUnQACXN0cmVhbS0wMXNyAA5qYXZhLmxhbmcuTG9uZzuL5JDMjyPfAgABSgAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAAAAAAAADHA=";
    StaticCheckpointGenerator deserializedGenerator =
        (StaticCheckpointGenerator) deSerializeObjectFromString(serializedGeneratorRelease2460);
    assertThat(generator.generate(null)).containsExactlyInAnyOrder(shardCheckpoint);
    assertThat(deserializedGenerator.generate(null)).containsExactlyInAnyOrder(shardCheckpoint);
    // This may be too strict and not actually necessary.
    // More important is that deserialized content matches.
    assertThat(serializedGenerator).isEqualTo(serializedGeneratorRelease2460);
  }

  private static String serializeObjectToString(Serializable o) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(o);
    oos.close();
    return Base64.getEncoder().encodeToString(outputStream.toByteArray());
  }

  private static Object deSerializeObjectFromString(String s)
      throws IOException, ClassNotFoundException {
    byte[] data = Base64.getDecoder().decode(s);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    Object o = ois.readObject();
    ois.close();
    return o;
  }
}
