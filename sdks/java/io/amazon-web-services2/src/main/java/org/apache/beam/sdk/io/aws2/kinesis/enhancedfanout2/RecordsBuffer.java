package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;

interface RecordsBuffer {
    boolean push(Record record);
    CustomOptional<Record> fetchOne();
}
