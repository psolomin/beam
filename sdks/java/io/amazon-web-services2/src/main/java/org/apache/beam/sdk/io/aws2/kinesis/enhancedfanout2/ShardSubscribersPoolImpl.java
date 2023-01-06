package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;


class ShardSubscribersPoolImpl implements ShardSubscribersPool {
    private final RecordsBuffer recordsBuffer;

    ShardSubscribersPoolImpl(
            RecordsBuffer recordsBuffer
    ) {
        this.recordsBuffer = recordsBuffer;
    }

    @Override
    public boolean start() {
        return false;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public CustomOptional<KinesisRecord> nextRecord() {
        // TODO: handle this in nicer way
        CustomOptional<Record> maybeRecord = recordsBuffer.fetchOne();
        if (maybeRecord.isPresent() && maybeRecord.get().getKinesisRecord().isPresent())
            return CustomOptional.of(maybeRecord.get().getKinesisRecord().get());
        else
            return CustomOptional.absent();
    }
}
