package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout2;

import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class RecordsBufferImpl implements RecordsBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(RecordsBufferImpl.class);

    private final int maxCapacity = 20_000;
    private final long pollTimeoutMs = 5_000L;
    private final RecordsBufferState state;
    private final BlockingQueue<Record> queue;

    RecordsBufferImpl(RecordsBufferState state) {
        this.state = state;
        this.queue = new LinkedBlockingQueue<>(maxCapacity);
    }

    @Override
    public boolean push(Record record) {
        return false;
    }

    @Override
    public CustomOptional<Record> fetchOne() {
        try {
            Record recordOrNull = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
            if (recordOrNull != null) {
                state.ackRecord(recordOrNull);
                return CustomOptional.of(recordOrNull);
            }
            else return CustomOptional.absent();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while fetching record");
            return CustomOptional.absent();
        }
    }
}
