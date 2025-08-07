package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.hbase.client.Mutation;

import java.io.Serializable;

/**
 * This class is used by {@link HBaseSink} and {@link HBaseWriter} to wrap HBase {@link Mutation}
 * objects to be able to serialize them.
 */
@Internal
public class SerializableMutation implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient Mutation mutation;

    public SerializableMutation(Mutation mutation) {
        this.mutation = mutation;
    }

    /** Get the wrapped mutation object. */
    public Mutation get() {
        return mutation;
    }
}
