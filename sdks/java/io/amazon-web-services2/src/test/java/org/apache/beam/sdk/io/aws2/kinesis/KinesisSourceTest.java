package org.apache.beam.sdk.io.aws2.kinesis;

import org.junit.Test;

import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.createReadSpec;
import static org.assertj.core.api.Assertions.assertThat;

public class KinesisSourceTest {
    @Test
    public void testSerialization() {
        KinesisIO.Read readSpec = createReadSpec();
        KinesisSource source = new KinesisSource(readSpec);
        assertThat(source).isNotNull();
    }
}
