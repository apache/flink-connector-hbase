package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.SerializableMutation;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sink implementation for writing data to Apache HBase tables using asynchronous batching.
 *
 * <p>This sink extends {@link AsyncSinkBase} to provide efficient, batched writes to HBase tables.
 * It buffers incoming records and writes them in configurable batches to optimize throughput and
 * reduce the number of round trips to HBase.
 *
 * <p>The sink supports configurable batching parameters including batch size limits, time-based
 * flushing, and request timeout handling. It uses an {@link ElementConverter} to transform input
 * records of type {@code T} into HBase mutations.
 *
 * <p><b>Note:</b> It is recommended to use {@link HBaseSinkBuilder} to create instances of this
 * class rather than calling the constructor directly. The builder provides a more convenient and
 * readable way to configure the sink:
 *
 * <pre>{@code
 * HBaseSink<MyRecord> sink = new HBaseSinkBuilder<MyRecord>()
 *     .setTableName("my_table")
 *     .setElementConverter(myElementConverter)
 *     .setMaxBatchSize(100)
 *     .setMaxInFlightRequests(5)
 *     .setMaxBufferedRequests(1000)
 *     .setMaxBatchSizeInBytes(1048576L)  // 1MB
 *     .setMaxTimeInBufferMS(5000L)       // 5 seconds
 *     .setMaxRecordSizeInBytes(10485760L) // 10MB
 *     .setRequestTimeoutMS(30000L)       // 30 seconds
 *     .setFailOnTimeout(false)
 *     .setConfiguration(hbaseConfiguration)
 *     .build();
 * }</pre>
 *
 * @param <T> The type of input elements to be written to HBase
 */
@PublicEvolving
public class HBaseSink<T> extends AsyncSinkBase<T, SerializableMutation> {

    /**
     * The converter used to transform input elements into HBase mutations wrapped by {@link
     * SerializableMutation}.
     */
    private final ElementConverter<T, SerializableMutation> elementConverter;

    /** The name of the HBase table to write to. */
    private final String tableName;

    /**
     * Serialized Hadoop configuration for HBase connection settings. Stored as a byte array to
     * ensure that the sink is serializable.
     */
    private final byte[] serializedHadoopConfiguration;

    public HBaseSink(
            ElementConverter<T, SerializableMutation> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            long requestTimeoutMS,
            boolean failOnTimeout,
            String tableName,
            Configuration configuration) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                requestTimeoutMS,
                failOnTimeout);

        checkNotNull(tableName, "HBase sink table name must not be null.");
        checkArgument(!tableName.isEmpty(), "HBase sink table name must not be empty.");
        checkNotNull(configuration, "HBase configuration must not be null.");

        this.elementConverter = elementConverter;
        this.tableName = tableName;
        this.serializedHadoopConfiguration =
                HBaseConfigurationUtil.serializeConfiguration(configuration);
    }

    /** This can be removed once rebased to Flink 2.0. */
    public SinkWriter<T> createWriter(InitContext context) {
        return new HBaseWriter<>(
                elementConverter,
                context,
                Collections.emptyList(),
                getMaxBatchSize(),
                getMaxBatchSizeInBytes(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                getRequestTimeoutMS(),
                getFailOnTimeout(),
                tableName,
                HBaseConfigurationUtil.deserializeConfiguration(
                        serializedHadoopConfiguration, null));
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext writerInitContext) {
        return new HBaseWriter<>(
                elementConverter,
                writerInitContext,
                Collections.emptyList(),
                getMaxBatchSize(),
                getMaxBatchSizeInBytes(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                getRequestTimeoutMS(),
                getFailOnTimeout(),
                tableName,
                HBaseConfigurationUtil.deserializeConfiguration(
                        serializedHadoopConfiguration, null));
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<SerializableMutation>> restoreWriter(
            WriterInitContext context,
            Collection<BufferedRequestState<SerializableMutation>> recoveredState) {
        return new HBaseWriter<>(
                elementConverter,
                context,
                recoveredState,
                getMaxBatchSize(),
                getMaxBatchSizeInBytes(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                getRequestTimeoutMS(),
                getFailOnTimeout(),
                tableName,
                HBaseConfigurationUtil.deserializeConfiguration(
                        serializedHadoopConfiguration, null));
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<SerializableMutation>>
            getWriterStateSerializer() {
        return new HBaseStateSerializer();
    }
}
