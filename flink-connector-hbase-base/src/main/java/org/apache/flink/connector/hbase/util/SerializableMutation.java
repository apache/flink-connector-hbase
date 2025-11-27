package org.apache.flink.connector.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.sink.HBaseWriter;

import org.apache.hadoop.hbase.client.Mutation;

import java.io.Serializable;

/**
 * This class is used by {@link HBaseSink} and {@link HBaseWriter} to wrap HBase {@link Mutation}
 * objects to be able to serialize them.
 */
@Internal
public class SerializableMutation implements Serializable {
    private static final long serialVersionUID = 1L;

    private final transient Mutation mutation;
    private long recordWriteAttempts;

    public SerializableMutation(Mutation mutation) {
        this(mutation, 0L);
    }

    public SerializableMutation(Mutation mutation, long recordWriteAttempts) {
        this.mutation = mutation;
        this.recordWriteAttempts = recordWriteAttempts;
    }

    /** Get the wrapped mutation object. */
    public Mutation get() {
        return mutation;
    }

    public long getRecordWriteAttempts() {
        return recordWriteAttempts;
    }

    public void incWriteAttempts() {
        recordWriteAttempts++;
    }
}
