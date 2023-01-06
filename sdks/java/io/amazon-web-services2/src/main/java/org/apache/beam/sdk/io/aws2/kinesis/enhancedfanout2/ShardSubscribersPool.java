package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;

interface ShardSubscribersPool {
    boolean start();
    boolean stop();
    CustomOptional<KinesisRecord> nextRecord();
}
