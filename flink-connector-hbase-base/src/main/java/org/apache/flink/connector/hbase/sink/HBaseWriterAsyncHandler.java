package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.hbase.util.SerializableMutation;
import org.apache.flink.metrics.Counter;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This class is responsible for managing the async calls to HBase and managing the {@link
 * ResultHandler} to decide which request can be retried.
 */
@Internal
public class HBaseWriterAsyncHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseWriterAsyncHandler.class);

    private final Counter numRecordsOutErrorsCounter;
    private final long maxRecordWriteAttempts;

    public HBaseWriterAsyncHandler(
            Counter numRecordsOutErrorsCounter, long maxRecordWriteAttempts) {
        this.numRecordsOutErrorsCounter = numRecordsOutErrorsCounter;
        this.maxRecordWriteAttempts = maxRecordWriteAttempts;
    }

    /**
     * For a given list of HBase write futures, this method will asynchronously analyze their
     * result, and using the provided {@link ResultHandler}, it will instruct {@link
     * org.apache.flink.connector.base.sink.writer.AsyncSinkWriter} to retry some mutations. In case
     * of errors which should not be retried, the Flink job will stop with an error.
     *
     * @param futures list of HBase write futures
     * @param processedMutationsInOrder list of mutations with their indices matching that of the
     *     futures
     * @param resultHandler result handler to manage retries and exceptions
     */
    public void handleWriteFutures(
            List<CompletableFuture<Mutation>> futures,
            List<SerializableMutation> processedMutationsInOrder,
            ResultHandler<SerializableMutation> resultHandler) {
        checkArgument(
                futures.size() == processedMutationsInOrder.size(),
                "Different number of HBase futures was supplied than mutations.");

        Queue<FailedMutation> failedMutations = new ConcurrentLinkedQueue<>();

        // Handle each future separately and store failures.
        CompletableFuture<?>[] handledFutures =
                IntStream.range(0, futures.size())
                        .mapToObj(
                                i ->
                                        addFailedCallback(
                                                futures.get(i),
                                                failedMutations,
                                                processedMutationsInOrder.get(i)))
                        .toArray(CompletableFuture[]::new);

        // Exceptions are already handled here, so it's safe to use `thenRun()`.
        CompletableFuture.allOf(handledFutures)
                .thenRun(() -> handleFailedRequests(failedMutations, resultHandler));
    }

    /**
     * For a {@link CompletableFuture} HBase write future, return a new future that adds a {@link
     * FailedMutation} to the failedMutations queue to mark that write as failed.
     *
     * @param cf future of a mutation write
     * @param failedMutations queue of failed mutations to add a new entry to upon failure
     * @param mutation the actual mutation that's being written by the future
     * @return new future with the exception callback
     */
    private CompletableFuture<Mutation> addFailedCallback(
            CompletableFuture<Mutation> cf,
            Queue<FailedMutation> failedMutations,
            SerializableMutation mutation) {

        return cf.exceptionally(
                throwable -> {
                    failedMutations.add(new FailedMutation(mutation, throwable));
                    return null;
                });
    }

    /**
     * Handles mutations that failed to write to HBase.
     *
     * <p>This method increments the error counter and schedules the failed mutations for retry
     * through the result handler. If the exception should not be retried, the job will fail instead
     * with an exception.
     *
     * @param failedMutations the list of mutations that failed to write
     * @param resultHandler the handler responsible for retry logic
     */
    private void handleFailedRequests(
            Collection<FailedMutation> failedMutations,
            ResultHandler<SerializableMutation> resultHandler) {
        if (failedMutations.isEmpty()) {
            resultHandler.complete();
            return;
        }

        numRecordsOutErrorsCounter.inc(failedMutations.size());

        List<SerializableMutation> retryEntries = new ArrayList<>();
        for (FailedMutation failedMutation : failedMutations) {
            LOG.warn("Mutation failed with exception", failedMutation.getThrowable());

            if (isHBaseExceptionFatal(failedMutation.getThrowable())) {
                resultHandler.completeExceptionally(
                        new HBaseSinkException.HBaseSinkMutationException(
                                failedMutation.getThrowable()));
                return;
            }

            SerializableMutation serializableMutation = failedMutation.getMutation();
            serializableMutation.incWriteAttempts();
            if (maxRecordWriteAttempts > 0
                    && serializableMutation.getRecordWriteAttempts() > maxRecordWriteAttempts) {
                resultHandler.completeExceptionally(
                        new HBaseSinkException.HBaseSinkMutationException(
                                failedMutation.getThrowable()));
                return;
            }

            retryEntries.add(serializableMutation);
        }

        resultHandler.retryForEntries(retryEntries);
    }

    /**
     * Check if HBase exception is fatal or could be retried. Also keeps a set of visited exceptions
     * to make sure prevent infinite recursion.
     */
    private boolean isHBaseExceptionFatal(Throwable throwable, Set<Throwable> visited) {
        if (throwable == null || !visited.add(throwable)) {
            // Null or already visited
            return false;
        }

        if (throwable instanceof DoNotRetryIOException) {
            return true;
        }

        return isHBaseExceptionFatal(throwable.getCause(), visited);
    }

    private boolean isHBaseExceptionFatal(Throwable throwable) {
        return isHBaseExceptionFatal(throwable, new HashSet<>());
    }

    /** Container class for a failed mutation also including the exception thrown. */
    private static final class FailedMutation {
        private final SerializableMutation mutation;
        private final Throwable throwable;

        private FailedMutation(SerializableMutation mutation, Throwable throwable) {
            this.mutation = mutation;
            this.throwable = throwable;
        }

        public SerializableMutation getMutation() {
            return mutation;
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }
}
