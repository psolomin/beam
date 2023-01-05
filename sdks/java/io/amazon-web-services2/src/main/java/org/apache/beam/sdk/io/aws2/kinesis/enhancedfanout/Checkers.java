package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import org.checkerframework.checker.nullness.qual.Nullable;

// TODO: this should be replaced with more standard checkers
class Checkers {
    static <T> T checkNotNull(@Nullable T reference, String objName) {
        if (reference == null) {
            throw new RuntimeException(objName + " is null");
        } else {
            return reference;
        }
    }
}
