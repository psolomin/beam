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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.StreamConsumer;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.Record;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.sink.RecordsSink;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class RecordsGenerators {
  static software.amazon.awssdk.services.kinesis.model.Record createRecord(
      AtomicInteger sequenceNumber) {
    return software.amazon.awssdk.services.kinesis.model.Record.builder()
        .partitionKey("foo")
        .approximateArrivalTimestamp(Instant.now())
        .sequenceNumber(String.valueOf(sequenceNumber.incrementAndGet()))
        .data(SdkBytes.fromByteArray(sequenceNumber.toString().getBytes(UTF_8)))
        .build();
  }

  static List<software.amazon.awssdk.services.kinesis.model.Record> createRecords(
      AtomicInteger sequenceNumber, int numberOfEvents) {
    return IntStream.range(0, numberOfEvents)
        .mapToObj(i -> createRecord(sequenceNumber))
        .collect(Collectors.toList());
  }

  static List<software.amazon.awssdk.services.kinesis.model.Record> createRecords(
      int numberOfEvents) {
    AtomicInteger sequenceNumber = new AtomicInteger(0);
    return createRecords(sequenceNumber, numberOfEvents);
  }

  public static List<KinesisClientRecord> createClientRecords(int numberOfEvents) {
    List<software.amazon.awssdk.services.kinesis.model.Record> rawRecords =
        createRecords(numberOfEvents);
    return rawRecords.stream().map(KinesisClientRecord::fromRecord).collect(Collectors.toList());
  }

  public static List<KinesisRecord> waitForRecords(
      StreamConsumer consumer, int expectedRecordsCnt, int maxAttempts) {
    int attemptNo = 0;
    List<KinesisRecord> records = new ArrayList<>();

    while (attemptNo < maxAttempts) {
      attemptNo++;
      CustomOptional<KinesisRecord> maybeRecord = consumer.nextRecord();
      if (maybeRecord.isPresent()) records.add(maybeRecord.get());
    }

    assertEquals(expectedRecordsCnt, records.size());
    return records;
  }

  public static List<KinesisRecord> waitForRecords(
      StreamConsumer consumer, int expectedRecordsCnt) {
    return waitForRecords(consumer, expectedRecordsCnt, expectedRecordsCnt * 3);
  }

  public static List<Record> waitForRecords(
      RecordsSink sink, int expectedRecordsCnt, int maxAttempts) {
    int attemptNo = 0;
    List<Record> records = new ArrayList<>();

    while (attemptNo < maxAttempts) {
      attemptNo++;
      Optional<Record> maybeRecord = sink.fetch();
      if (maybeRecord.isPresent()) records.add(maybeRecord.get());
    }

    assertEquals(expectedRecordsCnt, records.size());
    return records;
  }
}
