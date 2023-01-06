package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

interface RecordsBufferState {
    void ackRecord(Record record);
}
