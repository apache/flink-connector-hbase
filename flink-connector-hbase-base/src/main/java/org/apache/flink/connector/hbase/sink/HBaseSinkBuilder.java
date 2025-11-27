package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.hbase.util.SerializableMutation;
import org.apache.flink.connector.hbase.util.WrappedElementConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;

import java.util.Optional;

/**
 * A builder class for constructing {@link HBaseSink} instances with a fluent API.
 *
 * <p>This builder provides a convenient way to configure and create HBase sinks with proper default
 * values for all configuration parameters. It extends {@link AsyncSinkBaseBuilder} to inherit
 * common async sink configuration options while adding HBase-specific settings.
 *
 * <p>The builder ensures that all required parameters are set before building the sink and provides
 * sensible defaults for optional parameters:
 *
 * <ul>
 *   <li>maxBatchSize: 1000 records
 *   <li>maxInFlightRequests: 50 concurrent requests
 *   <li>maxBufferedRequests: 10000 records
 *   <li>maxBatchSizeInBytes: 2MB
 *   <li>maxTimeInBufferMS: 1 second
 *   <li>maxRecordSizeInBytes: 1MB
 *   <li>requestTimeoutMS: 60 seconds
 *   <li>failOnTimeout: false
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * HBaseSink<MyRecord> sink = new HBaseSinkBuilder<MyRecord>()
 *     .setTableName("my_table")
 *     .setElementConverter(myElementConverter)
 *     .setConfiguration(hbaseConfiguration)
 *     .setMaxBatchSize(100)
 *     .setMaxInFlightRequests(5)
 *     .setMaxBufferedRequests(1000)
 *     .setMaxBatchSizeInBytes(2097152L)  // 1MB
 *     .setMaxTimeInBufferMS(5000L)       // 5 seconds
 *     .setMaxRecordSizeInBytes(1048576L) // 10MB
 *     .setRequestTimeoutMS(30000L)       // 30 seconds
 *     .setFailOnTimeout(false)
 *     .build();
 * }</pre>
 *
 * @param <InputT> The type of input elements to be written to HBase
 */
@PublicEvolving
public class HBaseSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, SerializableMutation, HBaseSinkBuilder<InputT>> {

    private static final Integer DEFAULT_MAX_BATCH_SIZE = 1000;
    private static final Integer DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final Integer DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final Long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 2L * 1024L * 1024L;
    private static final Long DEFAULT_MAX_TIME_IN_BUFFER_MS = 1000L;
    private static final Long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 1024L * 1024L;
    private static final Long DEFAULT_MAX_REQUEST_TIMEOUT_MS = 60L * 1000L;
    private static final Boolean DEFAULT_FAIL_ON_TIMEOUT = false;
    private static final Long DEFAULT_MAX_RECORD_WRITE_ATTEMPTS = 0L;

    private String tableName;
    private Configuration configuration;
    private ElementConverter<InputT, SerializableMutation> elementConverter;
    private Long requestTimeoutMS = null;
    private Boolean failOnTimeout = null;
    private Long maxRecordWriteAttempts = null;

    public HBaseSinkBuilder() {}

    public HBaseSinkBuilder<InputT> setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public HBaseSinkBuilder<InputT> setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public HBaseSinkBuilder<InputT> setRequestTimeoutMS(Long requestTimeoutMS) {
        this.requestTimeoutMS = requestTimeoutMS;
        return this;
    }

    public HBaseSinkBuilder<InputT> setFailOnTimeout(Boolean failOnTimeout) {
        this.failOnTimeout = failOnTimeout;
        return this;
    }

    public HBaseSinkBuilder<InputT> setMaxRecordWriteAttempts(Long maxRecordWriteAttempts) {
        this.maxRecordWriteAttempts = maxRecordWriteAttempts;
        return this;
    }

    /**
     * Set up the converter to use when converting the input elements to a {@link Mutation} object.
     * Since {@link Mutation} objects don't implement {@link java.io.Serializable}, this will
     * internally wrap the passed {@link ElementConverter} to create a {@link SerializableMutation}
     * for the Sink.
     */
    public HBaseSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, Mutation> elementConverter) {
        this.elementConverter = new WrappedElementConverter<>(elementConverter);
        return this;
    }

    @Override
    public HBaseSink<InputT> build() {
        return new HBaseSink<>(
                elementConverter,
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes())
                        .orElse(DEFAULT_MAX_BATCH_SIZE_IN_BYTES),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes())
                        .orElse(DEFAULT_MAX_RECORD_SIZE_IN_BYTES),
                Optional.ofNullable(requestTimeoutMS).orElse(DEFAULT_MAX_REQUEST_TIMEOUT_MS),
                Optional.ofNullable(failOnTimeout).orElse(DEFAULT_FAIL_ON_TIMEOUT),
                Optional.ofNullable(maxRecordWriteAttempts)
                        .orElse(DEFAULT_MAX_RECORD_WRITE_ATTEMPTS),
                tableName,
                configuration);
    }
}
