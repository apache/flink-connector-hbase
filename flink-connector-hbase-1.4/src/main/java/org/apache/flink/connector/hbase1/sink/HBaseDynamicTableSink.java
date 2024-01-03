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

package org.apache.flink.connector.hbase1.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.hbase.sink.RowDataToMutationConverter;
import org.apache.flink.connector.hbase.sink.WritableMetadata;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** HBase table sink implementation. */
@Internal
public class HBaseDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final HBaseTableSchema hbaseTableSchema;
    private final String nullStringLiteral;
    private final Configuration hbaseConf;
    private final HBaseWriteOptions writeOptions;
    private final String tableName;
    private final DataType physicalDataType;

    /** Metadata that is appended at the end of a physical sink row. */
    private List<String> metadataKeys;

    public HBaseDynamicTableSink(
            String tableName,
            DataType physicalDataType,
            Configuration hbaseConf,
            HBaseWriteOptions writeOptions,
            String nullStringLiteral) {
        this.tableName = tableName;
        this.physicalDataType = physicalDataType;
        this.hbaseTableSchema = HBaseTableSchema.fromDataType(physicalDataType);
        this.metadataKeys = Collections.emptyList();
        this.hbaseConf = hbaseConf;
        this.writeOptions = writeOptions;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        HBaseSinkFunction<RowData> sinkFunction =
                new HBaseSinkFunction<>(
                        tableName,
                        hbaseConf,
                        new RowDataToMutationConverter(
                                hbaseTableSchema,
                                physicalDataType,
                                metadataKeys,
                                nullStringLiteral,
                                writeOptions.isIgnoreNullValue(),
                                writeOptions.isDynamicTable()),
                        writeOptions.getBufferFlushMaxSizeInBytes(),
                        writeOptions.getBufferFlushMaxRows(),
                        writeOptions.getBufferFlushIntervalMillis());
        return SinkFunctionProvider.of(sinkFunction, writeOptions.getParallelism());
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
                tableName, physicalDataType, hbaseConf, writeOptions, nullStringLiteral);
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
    public HBaseWriteOptions getWriteOptions() {
        return writeOptions;
    }

    @VisibleForTesting
    public Configuration getConfiguration() {
        return this.hbaseConf;
    }

    @VisibleForTesting
    public String getTableName() {
        return this.tableName;
    }
}
