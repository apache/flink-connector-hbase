package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.PublicEvolving;

/** Exception that can be thrown by {@link HBaseWriter} wrapping the HBase {@link Throwable}. */
@PublicEvolving
public class HBaseSinkException extends RuntimeException {

    public HBaseSinkException(String message) {
        super(message);
    }

    public HBaseSinkException(String message, Throwable throwable) {
        super(message, throwable);
    }

    /** Exception thrown during HBase sink initialization. */
    public static class HBaseSinkInitException extends HBaseSinkException {
        private static final String ERROR_MESSAGE =
                "Exception while trying to initialize HBase sink.";

        public HBaseSinkInitException(Throwable throwable) {
            super(ERROR_MESSAGE, throwable);
        }

        public HBaseSinkInitException(String message) {
            super(message);
        }
    }

    /** Exception thrown when trying to persist HBase mutations. */
    public static class HBaseSinkMutationException extends HBaseSinkException {
        private static final String ERROR_MESSAGE =
                "Exception while trying to persist records in HBase sink, not retrying.";

        public HBaseSinkMutationException(Throwable throwable) {
            super(ERROR_MESSAGE, throwable);
        }
    }
}
