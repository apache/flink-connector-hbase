package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.hbase.util.HBaseMutationSerialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer class for state in HBase sink. */
@Internal
public class HBaseStateSerializer extends AsyncSinkWriterStateSerializer<SerializableMutation> {
    @Override
    protected void serializeRequestToStream(SerializableMutation request, DataOutputStream out)
            throws IOException {
        HBaseMutationSerialization.serialize(request.get(), out);
    }

    @Override
    protected SerializableMutation deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        return new SerializableMutation(HBaseMutationSerialization.deserialize(in));
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
