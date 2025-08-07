package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * This is a helper class used to wrap an {@link ElementConverter} supplied by the user that
 * converts the input data to {@link Mutation}. With this class, the elements will be seamlessly
 * converted to internal {@link SerializableMutation} objects that can be serialized by the sink.
 */
@Internal
public class WrappedElementConverter<InputT>
        implements ElementConverter<InputT, SerializableMutation> {
    private static final long serialVersionUID = 1L;

    private final ElementConverter<InputT, Mutation> originalElementConverter;

    public WrappedElementConverter(ElementConverter<InputT, Mutation> originalElementConverter) {
        this.originalElementConverter = originalElementConverter;
    }

    @Override
    public void open(Sink.InitContext context) {
        ElementConverter.super.open(context);
        originalElementConverter.open(context);
    }

    @Override
    public SerializableMutation apply(InputT element, SinkWriter.Context context) {
        return new SerializableMutation(originalElementConverter.apply(element, context));
    }
}
