package org.apache.flink.connector.hbase.util;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Internal utility class for serializing and deserializing HBase mutations.
 *
 * <p>This class provides methods to convert HBase {@link Mutation} objects to and from their
 * Protocol Buffer representations for transmission over the wire or storage. It supports the
 * following HBase mutation types: {@link Put} and {@link Delete}.
 */
@Internal
public class HBaseMutationSerialization {
    public static void serialize(Mutation mutation, OutputStream out) throws IOException {
        ClientProtos.MutationProto.MutationType type;
        if (mutation instanceof Put) {
            type = ClientProtos.MutationProto.MutationType.PUT;
        } else if (mutation instanceof Delete) {
            type = ClientProtos.MutationProto.MutationType.DELETE;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown HBase mutation type, cannot serialize: %s",
                            mutation.getClass()));
        }

        ClientProtos.MutationProto proto = ProtobufUtil.toMutation(type, mutation);
        proto.writeDelimitedTo(out);
    }

    public static Mutation deserialize(InputStream in) throws IOException {
        ClientProtos.MutationProto proto = ClientProtos.MutationProto.parseDelimitedFrom(in);
        return ProtobufUtil.toMutation(proto);
    }
}
