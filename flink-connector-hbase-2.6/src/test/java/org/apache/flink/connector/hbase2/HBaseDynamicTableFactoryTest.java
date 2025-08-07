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

package org.apache.flink.connector.hbase2;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.hbase.source.HBaseRowDataLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase2.sink.HBaseDynamicTableSink;
import org.apache.flink.connector.hbase2.source.HBaseDynamicTableSource;
import org.apache.flink.connector.hbase2.source.HBaseRowDataAsyncLookupFunction;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;

import org.apache.hadoop.hbase.HConstants;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.LOOKUP_ASYNC;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_FAIL_ON_TIMEOUT;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_IGNORE_NULL_VALUE;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_MAX_RECORD_SIZE;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_REQUEST_TIMEOUT;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.MAX_RETRIES;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link HBase2DynamicTableFactory}. */
class HBaseDynamicTableFactoryTest {

    private static final String FAMILY1 = "f1";
    private static final String FAMILY2 = "f2";
    private static final String FAMILY3 = "f3";
    private static final String FAMILY4 = "f4";
    private static final String COL1 = "c1";
    private static final String COL2 = "c2";
    private static final String COL3 = "c3";
    private static final String COL4 = "c4";
    private static final String ROWKEY = "rowkey";

    @Test
    void testTableSourceFactory() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(FAMILY1, ROW(FIELD(COL1, INT()))),
                        Column.physical(FAMILY2, ROW(FIELD(COL1, INT()), FIELD(COL2, BIGINT()))),
                        Column.physical(ROWKEY, BIGINT()),
                        Column.physical(
                                FAMILY3,
                                ROW(
                                        FIELD(COL1, DOUBLE()),
                                        FIELD(COL2, BOOLEAN()),
                                        FIELD(COL3, STRING()))),
                        Column.physical(
                                FAMILY4,
                                ROW(
                                        FIELD(COL1, DECIMAL(10, 3)),
                                        FIELD(COL2, TIMESTAMP(3)),
                                        FIELD(COL3, DATE()),
                                        FIELD(COL4, TIME()))));

        DynamicTableSource source = createTableSource(schema, getAllOptions());
        assertThat(source).isInstanceOf(HBaseDynamicTableSource.class);
        HBaseDynamicTableSource hbaseSource = (HBaseDynamicTableSource) source;

        int[][] lookupKey = {{2}};
        LookupTableSource.LookupRuntimeProvider lookupProvider =
                hbaseSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        assertThat(lookupProvider).isInstanceOf(LookupFunctionProvider.class);

        LookupFunction tableFunction =
                ((LookupFunctionProvider) lookupProvider).createLookupFunction();
        assertThat(tableFunction).isInstanceOf(HBaseRowDataLookupFunction.class);
        assertThat(((HBaseRowDataLookupFunction) tableFunction).getHTableName())
                .isEqualTo("testHBaseTable");

        HBaseTableSchema hbaseSchema = hbaseSource.getHBaseTableSchema();
        assertThat(hbaseSchema.getRowKeyIndex()).isEqualTo(2);
        assertThat(hbaseSchema.getRowKeyTypeInfo()).contains(Types.LONG);

        assertThat(hbaseSchema.getFamilyNames()).containsExactly("f1", "f2", "f3", "f4");
        assertThat(hbaseSchema.getQualifierNames("f1")).containsExactly("c1");
        assertThat(hbaseSchema.getQualifierNames("f2")).containsExactly("c1", "c2");
        assertThat(hbaseSchema.getQualifierNames("f3")).containsExactly("c1", "c2", "c3");
        assertThat(hbaseSchema.getQualifierNames("f4")).containsExactly("c1", "c2", "c3", "c4");
        assertThat(hbaseSchema.getQualifierDataTypes("f1")).containsExactly(INT());
        assertThat(hbaseSchema.getQualifierDataTypes("f2")).containsExactly(INT(), BIGINT());
        assertThat(hbaseSchema.getQualifierDataTypes("f3"))
                .containsExactly(DOUBLE(), BOOLEAN(), STRING());
        assertThat(hbaseSchema.getQualifierDataTypes("f4"))
                .containsExactly(DECIMAL(10, 3), TIMESTAMP(3), DATE(), TIME());
    }

    @Test
    void testLookupOptions() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));
        Map<String, String> options = getAllOptions();
        options.put(CACHE_TYPE.key(), "PARTIAL");
        options.put(PARTIAL_CACHE_EXPIRE_AFTER_ACCESS.key(), "15213s");
        options.put(PARTIAL_CACHE_EXPIRE_AFTER_WRITE.key(), "18213s");
        options.put(PARTIAL_CACHE_MAX_ROWS.key(), "10000");
        options.put(PARTIAL_CACHE_CACHE_MISSING_KEY.key(), "false");
        options.put(MAX_RETRIES.key(), "15513");

        DynamicTableSource source = createTableSource(schema, options);
        HBaseDynamicTableSource hbaseSource = (HBaseDynamicTableSource) source;
        assertThat(((HBaseDynamicTableSource) source).getMaxRetryTimes()).isEqualTo(15513);
        assertThat(hbaseSource.getCache()).isInstanceOf(DefaultLookupCache.class);
        DefaultLookupCache cache = (DefaultLookupCache) hbaseSource.getCache();
        assertThat(cache)
                .isEqualTo(
                        DefaultLookupCache.newBuilder()
                                .expireAfterAccess(Duration.ofSeconds(15213))
                                .expireAfterWrite(Duration.ofSeconds(18213))
                                .maximumSize(10000)
                                .cacheMissingKey(false)
                                .build());
    }

    @Test
    void testTableSinkFactory() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, DOUBLE()), FIELD(COL2, INT()))),
                        Column.physical(FAMILY2, ROW(FIELD(COL1, INT()), FIELD(COL3, BIGINT()))),
                        Column.physical(
                                FAMILY3, ROW(FIELD(COL2, BOOLEAN()), FIELD(COL3, STRING()))),
                        Column.physical(
                                FAMILY4,
                                ROW(
                                        FIELD(COL1, DECIMAL(10, 3)),
                                        FIELD(COL2, TIMESTAMP(3)),
                                        FIELD(COL3, DATE()),
                                        FIELD(COL4, TIME()))));

        DynamicTableSink sink = createTableSink(schema, getAllOptions());
        assertThat(sink).isInstanceOf(HBaseDynamicTableSink.class);
        HBaseDynamicTableSink actualSink = (HBaseDynamicTableSink) sink;

        HBaseTableSchema hbaseSchema = actualSink.getHBaseTableSchema();
        assertThat(hbaseSchema.getRowKeyIndex()).isZero();
        assertThat(hbaseSchema.getRowKeyDataType()).contains(STRING());

        assertThat(hbaseSchema.getFamilyNames()).containsExactly("f1", "f2", "f3", "f4");
        assertThat(hbaseSchema.getQualifierNames("f1")).containsExactly("c1", "c2");
        assertThat(hbaseSchema.getQualifierNames("f2")).containsExactly("c1", "c3");
        assertThat(hbaseSchema.getQualifierNames("f3")).containsExactly("c2", "c3");
        assertThat(hbaseSchema.getQualifierNames("f4")).containsExactly("c1", "c2", "c3", "c4");
        assertThat(hbaseSchema.getQualifierDataTypes("f1")).containsExactly(DOUBLE(), INT());
        assertThat(hbaseSchema.getQualifierDataTypes("f2")).containsExactly(INT(), BIGINT());
        assertThat(hbaseSchema.getQualifierDataTypes("f3")).containsExactly(BOOLEAN(), STRING());
        assertThat(hbaseSchema.getQualifierDataTypes("f4"))
                .containsExactly(DECIMAL(10, 3), TIMESTAMP(3), DATE(), TIME());

        // verify hadoop Configuration
        org.apache.hadoop.conf.Configuration expectedConfiguration =
                HBaseConfigurationUtil.getHBaseConfiguration();
        expectedConfiguration.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181");
        expectedConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/flink");
        expectedConfiguration.set("hbase.security.authentication", "kerberos");

        // verify tableName
        assertThat(actualSink.getTableName()).isEqualTo("testHBaseTable");

        HBaseDynamicTableSink expectedSink =
                (HBaseDynamicTableSink)
                        HBaseDynamicTableSink.builder()
                                .setPhysicalDataType(schema.toPhysicalRowDataType())
                                .setConfiguration(expectedConfiguration)
                                .build();
        assertThat(actualSink)
                .usingRecursiveComparison()
                .comparingOnlyFields("configuration")
                .isEqualTo(expectedSink);
    }

    @Test
    void testAsyncOptions() {
        Map<String, String> options = getAllOptions();
        options.put(MAX_BATCH_SIZE.key(), "100");
        options.put(MAX_IN_FLIGHT_REQUESTS.key(), "62");
        options.put(MAX_BUFFERED_REQUESTS.key(), "72");
        options.put(FLUSH_BUFFER_SIZE.key(), String.valueOf(10 * 1024 * 1024));
        options.put(FLUSH_BUFFER_TIMEOUT.key(), "10000");
        options.put(SINK_REQUEST_TIMEOUT.key(), "5 s");
        options.put(SINK_FAIL_ON_TIMEOUT.key(), "true");
        options.put(SINK_IGNORE_NULL_VALUE.key(), "true");
        options.put(SINK_MAX_RECORD_SIZE.key(), "6123");

        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));

        DynamicTableSink actualSink = createTableSink(schema, options);

        HBaseDynamicTableSink expectedSink =
                (HBaseDynamicTableSink)
                        HBaseDynamicTableSink.builder()
                                .setMaxBatchSize(100)
                                .setMaxInFlightRequests(62)
                                .setMaxBufferedRequests(72)
                                .setMaxBufferSizeInBytes(10 * 1024 * 1024)
                                .setMaxTimeInBufferMS(10 * 1000)
                                .setRequestTimeoutMS(5L * 1000L)
                                .setMaxRecordSizeInBytes(6123L)
                                .setFailOnTimeout(true)
                                .setIgnoreNullValue(true)
                                .setPhysicalDataType(schema.toPhysicalRowDataType())
                                .build();
        assertThat(actualSink)
                .usingRecursiveComparison()
                .comparingOnlyFields(
                        "maxBatchSize",
                        "maxInFlightRequests",
                        "maxBufferedRequests",
                        "maxBufferSizeInBytes",
                        "maxTimeInBufferMS",
                        "requestTimeoutMS",
                        "maxRecordSizeInBytes",
                        "failOnTimeout",
                        "ignoreNullValue")
                .isEqualTo(expectedSink);
    }

    @Test
    void testDeprecatedAsyncOptions() {
        Map<String, String> options = getAllOptions();
        options.put(SINK_BUFFER_FLUSH_MAX_SIZE.key(), "5");
        options.put(SINK_BUFFER_FLUSH_MAX_ROWS.key(), "6");
        options.put(SINK_BUFFER_FLUSH_INTERVAL.key(), "7");

        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));

        DynamicTableSink actualSink = createTableSink(schema, options);

        HBaseDynamicTableSink expectedSink =
                (HBaseDynamicTableSink)
                        HBaseDynamicTableSink.builder()
                                .setMaxBufferSizeInBytes(5)
                                .setMaxBatchSize(6)
                                .setMaxTimeInBufferMS(7L)
                                .setPhysicalDataType(schema.toPhysicalRowDataType())
                                .build();
        assertThat(actualSink)
                .usingRecursiveComparison()
                .comparingOnlyFields("maxBufferSizeInBytes", "maxBatchSize", "maxTimeInBufferMS")
                .isEqualTo(expectedSink);
    }

    @Test
    void testParallelismOptions() {
        Map<String, String> options = getAllOptions();
        options.put(SINK_PARALLELISM.key(), "2");

        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));

        DynamicTableSink sink = createTableSink(schema, options);
        assertThat(sink).isInstanceOf(HBaseDynamicTableSink.class);
        HBaseDynamicTableSink hbaseSink = (HBaseDynamicTableSink) sink;
        SinkV2Provider provider =
                (SinkV2Provider)
                        hbaseSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider.getParallelism()).contains(2);
    }

    @Test
    void testLookupAsync() {
        Map<String, String> options = getAllOptions();
        options.put(LOOKUP_ASYNC.key(), "true");
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, DOUBLE()), FIELD(COL2, INT()))));
        DynamicTableSource source = createTableSource(schema, options);
        assertThat(source).isInstanceOf(HBaseDynamicTableSource.class);
        HBaseDynamicTableSource hbaseSource = (HBaseDynamicTableSource) source;

        int[][] lookupKey = {{0}};
        LookupTableSource.LookupRuntimeProvider lookupProvider =
                hbaseSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        assertThat(lookupProvider).isInstanceOf(AsyncLookupFunctionProvider.class);

        AsyncLookupFunction asyncTableFunction =
                ((AsyncLookupFunctionProvider) lookupProvider).createAsyncLookupFunction();
        assertThat(asyncTableFunction).isInstanceOf(HBaseRowDataAsyncLookupFunction.class);
        assertThat(((HBaseRowDataAsyncLookupFunction) asyncTableFunction).getHTableName())
                .isEqualTo("testHBaseTable");
    }

    @Test
    void testUnknownOption() {
        Map<String, String> options = getAllOptions();
        options.put("sink.unknown.key", "unknown-value");
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, DOUBLE()), FIELD(COL2, INT()))));

        assertThatThrownBy(() -> createTableSource(schema, options))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "Unsupported options:\n\nsink.unknown.key"));
        assertThatThrownBy(() -> createTableSink(schema, options))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "Unsupported options:\n\nsink.unknown.key"));
    }

    @Test
    void testTypeWithUnsupportedPrecision() {
        Map<String, String> options = getAllOptions();
        // test unsupported timestamp precision
        ResolvedSchema wrongTs =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(
                                FAMILY1, ROW(FIELD(COL1, TIMESTAMP(6)), FIELD(COL2, INT()))));

        assertThatThrownBy(() -> createTableSource(wrongTs, options))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "The precision 6 of TIMESTAMP type is out of the range [0, 3] supported by HBase connector"));
        assertThatThrownBy(() -> createTableSink(wrongTs, options))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "The precision 6 of TIMESTAMP type is out of the range [0, 3] supported by HBase connector"));

        // test unsupported time precision
        ResolvedSchema wrongTimeSchema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, TIME(6)), FIELD(COL2, INT()))));

        assertThatThrownBy(() -> createTableSource(wrongTimeSchema, options))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "The precision 6 of TIME type is out of the range [0, 3] supported by HBase connector"));
        assertThatThrownBy(() -> createTableSink(wrongTimeSchema, options))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "The precision 6 of TIME type is out of the range [0, 3] supported by HBase connector"));
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "hbase-2.6");
        options.put("table-name", "testHBaseTable");
        options.put("zookeeper.quorum", "localhost:2181");
        options.put("zookeeper.znode.parent", "/flink");
        options.put("properties.hbase.security.authentication", "kerberos");
        return options;
    }
}
