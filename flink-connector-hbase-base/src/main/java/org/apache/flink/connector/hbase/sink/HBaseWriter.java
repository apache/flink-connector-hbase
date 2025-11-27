package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.SerializableMutation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Sink writer created for {@link HBaseSink} to write {@link SerializableMutation} elements to
 * HBase. More details can be found in the JavaDocs for {@link HBaseSink}.
 *
 * <p>More details on the internals of this sink writer can be found in the JavaDocs of {@link
 * AsyncSinkWriter}.
 */
@Internal
public class HBaseWriter<InputT> extends AsyncSinkWriter<InputT, SerializableMutation> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);

    private final AsyncConnection connection;
    private final AsyncTable<AdvancedScanResultConsumer> table;
    private final SinkWriterMetricGroup metrics;
    private final Counter numRecordsOutErrorsCounter;
    private final HBaseWriterAsyncHandler hBaseWriterAsyncHandler;

    /** This can be removed once rebased to Flink 2.0. */
    public HBaseWriter(
            ElementConverter<InputT, SerializableMutation> elementConverter,
            Sink.InitContext context,
            Collection<BufferedRequestState<SerializableMutation>> states,
            int maxBatchSize,
            long maxBatchSizeInBytes,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            long requestTimeoutMS,
            boolean failOnTimeout,
            long maxRecordWriteAttempts,
            String tableName,
            Configuration configuration) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .setRequestTimeoutMS(requestTimeoutMS)
                        .setFailOnTimeout(failOnTimeout)
                        .setRateLimitingStrategy(
                                buildRateLimitingStrategy(maxInFlightRequests, maxBatchSize))
                        .build(),
                states);

        this.connection = createClient(configuration);
        this.table = connection.getTable(TableName.valueOf(tableName));
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.hBaseWriterAsyncHandler =
                new HBaseWriterAsyncHandler(numRecordsOutErrorsCounter, maxRecordWriteAttempts);
    }

    public HBaseWriter(
            ElementConverter<InputT, SerializableMutation> elementConverter,
            WriterInitContext context,
            Collection<BufferedRequestState<SerializableMutation>> states,
            int maxBatchSize,
            long maxBatchSizeInBytes,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            long requestTimeoutMS,
            boolean failOnTimeout,
            long maxRecordWriteAttempts,
            String tableName,
            Configuration configuration) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .setRequestTimeoutMS(requestTimeoutMS)
                        .setFailOnTimeout(failOnTimeout)
                        .setRateLimitingStrategy(
                                buildRateLimitingStrategy(maxInFlightRequests, maxBatchSize))
                        .build(),
                states);

        this.connection = createClient(configuration);
        this.table = connection.getTable(TableName.valueOf(tableName));
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.hBaseWriterAsyncHandler =
                new HBaseWriterAsyncHandler(numRecordsOutErrorsCounter, maxRecordWriteAttempts);
    }

    /**
     * Submits a batch of mutation requests to HBase asynchronously.
     *
     * <p>This method performs the following operations:
     *
     * <ol>
     *   <li>Deduplicates the request entries to ensure only the latest mutation per row is sent
     *   <li>Extracts {@link Mutation} from {@link SerializableMutation} entries
     *   <li>Submits the batch to HBase asynchronously
     *   <li>Handles failures by collecting failed mutations for retry
     * </ol>
     */
    @Override
    protected void submitRequestEntries(
            List<SerializableMutation> requestEntries,
            ResultHandler<SerializableMutation> resultHandler) {
        // Requests have to be deduplicated to ensure correct behavior.
        List<SerializableMutation> requestEntriesDeduplicated =
                deduplicateRequestEntries(requestEntries);

        // Convert WrappedMutations to Mutations
        List<Mutation> mutations =
                requestEntriesDeduplicated.stream()
                        .map(SerializableMutation::get)
                        .collect(Collectors.toList());

        // Handle failed requests to retry them later. It's possible that some mutations failed
        // while others did not.
        List<CompletableFuture<Mutation>> futures = table.batch(mutations);
        hBaseWriterAsyncHandler.handleWriteFutures(
                futures, requestEntriesDeduplicated, resultHandler);
    }

    @Override
    protected long getSizeInBytes(SerializableMutation requestEntry) {
        return requestEntry.get().heapSize();
    }

    @Override
    public void close() {
        if (!connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Group mutations and keep the latest mutation only. Please see <a
     * href="https://issues.apache.org/jira/browse/HBASE-8626">HBASE-8626</a> for more information.
     *
     * @param requestEntries entries to save
     * @return deduplicated entries with the latest mutation only for each affected row
     */
    private static List<SerializableMutation> deduplicateRequestEntries(
            List<SerializableMutation> requestEntries) {
        Map<ByteBuffer, SerializableMutation> requestEntriesMap = new HashMap<>();
        for (SerializableMutation requestEntry : requestEntries) {
            ByteBuffer key = ByteBuffer.wrap(requestEntry.get().getRow());
            SerializableMutation old = requestEntriesMap.get(key);
            if (old == null || requestEntry.get().getTimestamp() >= old.get().getTimestamp()) {
                requestEntriesMap.put(key, requestEntry);
            }
        }

        return new ArrayList<>(requestEntriesMap.values());
    }

    /** Builds a congestion control rate limiting strategy using AIMD algorithm. */
    private static RateLimitingStrategy buildRateLimitingStrategy(
            int maxInFlightRequests, int maxBatchSize) {
        return CongestionControlRateLimitingStrategy.builder()
                .setMaxInFlightRequests(maxInFlightRequests)
                .setInitialMaxInFlightMessages(maxBatchSize)
                .setScalingStrategy(
                        AIMDScalingStrategy.builder(maxBatchSize * maxInFlightRequests).build())
                .build();
    }

    /**
     * Creates an asynchronous HBase client connection using the provided configuration.
     *
     * <p>This method merges the runtime HBase configuration with the provided configuration and
     * validates that the ZooKeeper quorum is properly configured before establishing the
     * connection.
     *
     * @param configuration the HBase configuration to use for connection
     * @return an asynchronous connection to the HBase cluster
     * @throws HBaseSinkException.HBaseSinkInitException if ZooKeeper quorum is not configured or
     *     connection fails
     */
    private static AsyncConnection createClient(Configuration configuration)
            throws HBaseSinkException.HBaseSinkInitException {
        Configuration runtimeConfig = HBaseConfigurationUtil.getHBaseConfiguration();
        runtimeConfig.addResource(configuration);

        if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
            LOG.error(
                    "Can not connect to HBase without '{}' configuration",
                    HConstants.ZOOKEEPER_QUORUM);
            throw new HBaseSinkException.HBaseSinkInitException(
                    "Can not connect to HBase without '"
                            + HConstants.ZOOKEEPER_QUORUM
                            + "' configuration");
        }

        try {
            return ConnectionFactory.createAsyncConnection(runtimeConfig).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new HBaseSinkException.HBaseSinkInitException(e);
        }
    }
}
