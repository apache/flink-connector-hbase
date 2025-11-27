/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase2.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.hbase.sink.HBaseSinkBuilder;
import org.apache.flink.connector.hbase.sink.RowDataToMutationElementConverter;
import org.apache.flink.connector.hbase.sink.WritableMetadata;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase.util.SerializableMutation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** HBase table sink implementation. */
@Internal
public class HBaseDynamicTableSink extends AsyncDynamicTableSink<SerializableMutation>
        implements SupportsWritingMetadata {

    private final Long maxRecordSizeInBytes;
    private final Long requestTimeoutMS;
    private final Boolean failOnTimeout;
    private final Long maxRecordWriteAttempts;
    private final String tableName;
    private final HBaseTableSchema hbaseTableSchema;
    private final Configuration configuration;
    private final String nullStringLiteral;
    private final DataType physicalDataType;
    private final Integer parallelism;
    private final Boolean ignoreNullValue;

    /** Metadata that is appended at the end of a physical sink row. */
    private List<String> metadataKeys;

    public static HBaseDynamicSinkBuilder builder() {
        return new HBaseDynamicSinkBuilder();
    }

    public HBaseDynamicTableSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            Long requestTimeoutMS,
            Boolean failOnTimeout,
            Long maxRecordWriteAttempts,
            Integer parallelism,
            Boolean ignoreNullValue,
            String tableName,
            DataType physicalDataType,
            Configuration configuration,
            String nullStringLiteral) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
        this.requestTimeoutMS = requestTimeoutMS;
        this.failOnTimeout = failOnTimeout;
        this.maxRecordWriteAttempts = maxRecordWriteAttempts;
        this.parallelism = parallelism;
        this.ignoreNullValue = ignoreNullValue;
        this.tableName = tableName;
        this.physicalDataType = physicalDataType;
        this.hbaseTableSchema = HBaseTableSchema.fromDataType(physicalDataType);
        this.metadataKeys = Collections.emptyList();
        this.configuration = configuration;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public DynamicTableSink.SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        ElementConverter<RowData, Mutation> elementConverter =
                new RowDataToMutationElementConverter(
                        hbaseTableSchema,
                        physicalDataType,
                        metadataKeys,
                        nullStringLiteral,
                        ignoreNullValue);
        HBaseSinkBuilder<RowData> builder =
                new HBaseSinkBuilder<RowData>()
                        .setTableName(tableName)
                        .setConfiguration(configuration)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .setRequestTimeoutMS(requestTimeoutMS)
                        .setFailOnTimeout(failOnTimeout)
                        .setMaxRecordWriteAttempts(maxRecordWriteAttempts)
                        .setElementConverter(elementConverter);
        addAsyncOptionsToSinkBuilder(builder);
        return SinkV2Provider.of(builder.build(), parallelism);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return WritableMetadata.list();
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public DynamicTableSink copy() {
        return new HBaseDynamicTableSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                requestTimeoutMS,
                maxRecordSizeInBytes,
                failOnTimeout,
                maxRecordWriteAttempts,
                parallelism,
                ignoreNullValue,
                tableName,
                physicalDataType,
                configuration,
                nullStringLiteral);
    }

    @Override
    public String asSummaryString() {
        return "HBase";
    }

    // -------------------------------------------------------------------------------------------

    @VisibleForTesting
    public HBaseTableSchema getHBaseTableSchema() {
        return this.hbaseTableSchema;
    }

    @VisibleForTesting
    public Configuration getConfiguration() {
        return this.configuration;
    }

    @VisibleForTesting
    public String getTableName() {
        return this.tableName;
    }

    /** Class for building HBaseDynamicTableSink. */
    @Internal
    public static class HBaseDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<SerializableMutation, HBaseDynamicSinkBuilder> {

        private Long maxRecordSizeInBytes = null;
        private Long requestTimeoutMS = null;
        private Boolean failOnTimeout = null;
        private Long maxRecordWriteAttempts = null;
        private String tableName = null;
        private Configuration configuration = null;
        private String nullStringLiteral = null;
        private DataType physicalDataType = null;
        private Integer parallelism = null;
        private Boolean ignoreNullValue = null;

        @Override
        public AsyncDynamicTableSink<SerializableMutation> build() {
            return new HBaseDynamicTableSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    maxRecordSizeInBytes,
                    requestTimeoutMS,
                    failOnTimeout,
                    maxRecordWriteAttempts,
                    parallelism,
                    ignoreNullValue,
                    tableName,
                    physicalDataType,
                    configuration,
                    nullStringLiteral);
        }

        public HBaseDynamicSinkBuilder setMaxRecordSizeInBytes(Long maxRecordSizeInBytes) {
            this.maxRecordSizeInBytes = maxRecordSizeInBytes;
            return this;
        }

        public HBaseDynamicSinkBuilder setRequestTimeoutMS(Long requestTimeoutMS) {
            this.requestTimeoutMS = requestTimeoutMS;
            return this;
        }

        public HBaseDynamicSinkBuilder setFailOnTimeout(Boolean failOnTimeout) {
            this.failOnTimeout = failOnTimeout;
            return this;
        }

        public HBaseDynamicSinkBuilder setMaxRecordWriteAttempts(Long maxRecordWriteAttempts) {
            this.maxRecordWriteAttempts = maxRecordWriteAttempts;
            return this;
        }

        public HBaseDynamicSinkBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public HBaseDynamicSinkBuilder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public HBaseDynamicSinkBuilder setNullStringLiteral(String nullStringLiteral) {
            this.nullStringLiteral = nullStringLiteral;
            return this;
        }

        public HBaseDynamicSinkBuilder setPhysicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        public HBaseDynamicSinkBuilder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public HBaseDynamicSinkBuilder setIgnoreNullValue(Boolean ignoreNullValue) {
            this.ignoreNullValue = ignoreNullValue;
            return this;
        }
    }
}
