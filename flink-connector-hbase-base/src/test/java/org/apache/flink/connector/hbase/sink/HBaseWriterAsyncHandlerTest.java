package org.apache.flink.connector.hbase.sink;

import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.hbase.util.SerializableMutation;
import org.apache.flink.metrics.Counter;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HBaseWriterAsyncHandler}. */
class HBaseWriterAsyncHandlerTest {

    private final Counter errorCounter = new TestCounter();
    private final TestResultHandler resultHandler = new TestResultHandler();
    private final HBaseWriterAsyncHandler handler = new HBaseWriterAsyncHandler(errorCounter, 1);

    @Test
    void testHBaseWriteAsyncHandlerEmpty() {
        handler.handleWriteFutures(Collections.emptyList(), Collections.emptyList(), resultHandler);

        assertThat(errorCounter.getCount()).isEqualTo(0);
        assertThat(resultHandler.getComplete()).isTrue();
    }

    @Test
    void testHBaseWriteAsyncHandlerSuccessful() {
        List<SerializableMutation> mutations =
                IntStream.range(0, 500)
                        .mapToObj(__ -> generateMutation())
                        .collect(Collectors.toList());
        List<CompletableFuture<Mutation>> futures =
                mutations.stream()
                        .map(m -> new CompletableFuture<Mutation>())
                        .collect(Collectors.toList());

        handler.handleWriteFutures(futures, mutations, resultHandler);
        futures.forEach(f -> f.complete(null));

        assertThat(errorCounter.getCount()).isEqualTo(0);
        assertThat(resultHandler.getComplete()).isTrue();
    }

    /** Half the mutations will throw an exception that can be retried. */
    @Test
    void testHBaseWriteAsyncHandlerException() {
        List<SerializableMutation> allMutations =
                IntStream.range(0, 500)
                        .mapToObj(__ -> generateMutation())
                        .collect(Collectors.toList());
        List<CompletableFuture<Mutation>> futures =
                IntStream.range(0, 500)
                        .mapToObj(__ -> new CompletableFuture<Mutation>())
                        .collect(Collectors.toList());

        handler.handleWriteFutures(futures, allMutations, resultHandler);

        List<SerializableMutation> failedMutations = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            if (i % 2 == 0) {
                futures.get(i).complete(null);
            } else {
                futures.get(i)
                        .completeExceptionally(
                                new HBaseSinkException.HBaseSinkMutationException(
                                        new RuntimeException("test")));
                failedMutations.add(allMutations.get(i));
            }
        }

        assertThat(resultHandler.getEntriesRetried())
                .hasSameElementsAs(failedMutations)
                .allSatisfy(m -> assertThat(m.getRecordWriteAttempts()).isEqualTo(1));
        assertThat(errorCounter.getCount()).isEqualTo(250);
    }

    /** Some record will exceed the maximum write attempts. */
    @Test
    void testHBaseWriteAsyncHandlerMaxWriteAttempts() {
        List<SerializableMutation> allMutations =
                IntStream.range(0, 10)
                        .mapToObj(__ -> generateMutation())
                        .collect(Collectors.toList());

        // Write attempts will be 1, which is the threshold, so it should fail.
        allMutations.get(9).incWriteAttempts();

        List<CompletableFuture<Mutation>> futures =
                IntStream.range(0, 10)
                        .mapToObj(__ -> new CompletableFuture<Mutation>())
                        .collect(Collectors.toList());

        handler.handleWriteFutures(futures, allMutations, resultHandler);

        for (int i = 0; i < 9; i++) {
            futures.get(i).complete(null);
        }
        futures.get(9)
                .completeExceptionally(
                        new HBaseSinkException.HBaseSinkMutationException(
                                new RuntimeException("test")));

        assertThat(errorCounter.getCount()).isEqualTo(1);
        assertThat(resultHandler.getEntriesRetried()).isNull();
        assertThat(resultHandler.getException())
                .isExactlyInstanceOf(HBaseSinkException.HBaseSinkMutationException.class)
                .hasRootCauseExactlyInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("test");
    }

    /** Exactly one mutation will throw an exception that cannot be retried, the job should fail. */
    @Test
    void testHBaseWriteAsyncHandlerUnrecoverableException() {
        List<SerializableMutation> mutations =
                IntStream.range(0, 500)
                        .mapToObj(__ -> generateMutation())
                        .collect(Collectors.toList());
        List<CompletableFuture<Mutation>> futures =
                IntStream.range(0, 500)
                        .mapToObj(__ -> new CompletableFuture<Mutation>())
                        .collect(Collectors.toList());

        handler.handleWriteFutures(futures, mutations, resultHandler);
        for (int i = 0; i < futures.size(); i++) {
            if (i == 250) {
                futures.get(i)
                        .completeExceptionally(
                                new HBaseSinkException.HBaseSinkMutationException(
                                        new DoNotRetryIOException("test")));
            } else {
                futures.get(i).complete(null);
            }
        }

        assertThat(errorCounter.getCount()).isEqualTo(1);
        assertThat(resultHandler.getException())
                .isExactlyInstanceOf(HBaseSinkException.HBaseSinkMutationException.class)
                .hasRootCauseExactlyInstanceOf(DoNotRetryIOException.class)
                .hasMessage(
                        "Exception while trying to persist records in HBase sink, not retrying.");
    }

    private SerializableMutation generateMutation() {
        return new SerializableMutation(new Put(Bytes.toBytes(UUID.randomUUID().toString())));
    }

    /** Test class to verify usage of {@link ResultHandler}. */
    static final class TestResultHandler implements ResultHandler<SerializableMutation> {
        private Boolean complete = false;
        private Exception exception = null;
        private List<SerializableMutation> entriesRetried = null;

        @Override
        public void complete() {
            complete = true;
            exception = null;
            entriesRetried = null;
        }

        @Override
        public void completeExceptionally(Exception e) {
            exception = e;
            complete = false;
            entriesRetried = null;
        }

        @Override
        public void retryForEntries(List<SerializableMutation> requestEntriesToRetry) {
            entriesRetried = requestEntriesToRetry;
            complete = false;
            exception = null;
        }

        public Boolean getComplete() {
            return complete;
        }

        public Exception getException() {
            return exception;
        }

        public List<SerializableMutation> getEntriesRetried() {
            return entriesRetried;
        }
    }

    /** Test class to verify metrics. */
    static final class TestCounter implements Counter {
        private long countValue;

        public TestCounter() {
            this.countValue = 0;
        }

        @Override
        public void inc() {
            countValue++;
        }

        @Override
        public void inc(long n) {
            countValue += n;
        }

        @Override
        public void dec() {
            countValue--;
        }

        @Override
        public void dec(long n) {
            countValue -= n;
        }

        @Override
        public long getCount() {
            return countValue;
        }
    }
}
