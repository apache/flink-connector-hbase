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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.hbase.sink.RowDataToMutationConverter;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase2.source.AbstractTableInputFormat;
import org.apache.flink.connector.hbase2.source.HBaseRowDataInputFormat;
import org.apache.flink.connector.hbase2.util.HBaseTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for HBase connector (including source and sink). */
class HBaseConnectorITCase extends HBaseTestBase {

    // -------------------------------------------------------------------------------------
    // HBaseTableSource tests
    // -------------------------------------------------------------------------------------

    @Test
    void testTableSourceFullScan() {
        TableEnvironment tEnv = TableEnvironment.create(batchSettings);

        tEnv.executeSql(
                "CREATE TABLE hTable ("
                        + " family1 ROW<col1 INT>,"
                        + " family2 ROW<col1 STRING, col2 BIGINT>,"
                        + " family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,"
                        + " rowkey INT,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_1
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        Table table =
                tEnv.sqlQuery(
                        "SELECT "
                                + "  h.family1.col1, "
                                + "  h.family2.col1, "
                                + "  h.family2.col2, "
                                + "  h.family3.col1, "
                                + "  h.family3.col2, "
                                + "  h.family3.col3 "
                                + "FROM hTable AS h");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[10, Hello-1, 100, 1.01, false, Welt-1]\n"
                        + "+I[20, Hello-2, 200, 2.02, true, Welt-2]\n"
                        + "+I[30, Hello-3, 300, 3.03, false, Welt-3]\n"
                        + "+I[40, null, 400, 4.04, true, Welt-4]\n"
                        + "+I[50, Hello-5, 500, 5.05, false, Welt-5]\n"
                        + "+I[60, Hello-6, 600, 6.06, true, Welt-6]\n"
                        + "+I[70, Hello-7, 700, 7.07, false, Welt-7]\n"
                        + "+I[80, null, 800, 8.08, true, Welt-8]\n";

        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    void testTableSourceEmptyTableScan() {
        TableEnvironment tEnv = TableEnvironment.create(batchSettings);

        tEnv.executeSql(
                "CREATE TABLE hTable ("
                        + " family1 ROW<col1 INT>,"
                        + " rowkey INT,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_EMPTY_TABLE
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT rowkey, h.family1.col1 FROM hTable AS h");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());

        assertThat(results).isEmpty();
    }

    @Test
    void testTableSourceProjection() {
        TableEnvironment tEnv = TableEnvironment.create(batchSettings);

        tEnv.executeSql(
                "CREATE TABLE hTable ("
                        + " family1 ROW<col1 INT>,"
                        + " family2 ROW<col1 STRING, col2 BIGINT>,"
                        + " family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,"
                        + " rowkey INT,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_1
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        Table table =
                tEnv.sqlQuery(
                        "SELECT "
                                + "  h.family1.col1, "
                                + "  h.family3.col1, "
                                + "  h.family3.col2, "
                                + "  h.family3.col3 "
                                + "FROM hTable AS h");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[10, 1.01, false, Welt-1]\n"
                        + "+I[20, 2.02, true, Welt-2]\n"
                        + "+I[30, 3.03, false, Welt-3]\n"
                        + "+I[40, 4.04, true, Welt-4]\n"
                        + "+I[50, 5.05, false, Welt-5]\n"
                        + "+I[60, 6.06, true, Welt-6]\n"
                        + "+I[70, 7.07, false, Welt-7]\n"
                        + "+I[80, 8.08, true, Welt-8]\n";

        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    void testTableSourceFieldOrder() {
        TableEnvironment tEnv = TableEnvironment.create(batchSettings);

        tEnv.executeSql(
                "CREATE TABLE hTable ("
                        + " rowkey INT PRIMARY KEY NOT ENFORCED,"
                        + " family2 ROW<col1 STRING, col2 BIGINT>,"
                        + " family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,"
                        + " family1 ROW<col1 INT>"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_1
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT * FROM hTable AS h");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[1, +I[Hello-1, 100], +I[1.01, false, Welt-1], +I[10]]\n"
                        + "+I[2, +I[Hello-2, 200], +I[2.02, true, Welt-2], +I[20]]\n"
                        + "+I[3, +I[Hello-3, 300], +I[3.03, false, Welt-3], +I[30]]\n"
                        + "+I[4, +I[null, 400], +I[4.04, true, Welt-4], +I[40]]\n"
                        + "+I[5, +I[Hello-5, 500], +I[5.05, false, Welt-5], +I[50]]\n"
                        + "+I[6, +I[Hello-6, 600], +I[6.06, true, Welt-6], +I[60]]\n"
                        + "+I[7, +I[Hello-7, 700], +I[7.07, false, Welt-7], +I[70]]\n"
                        + "+I[8, +I[null, 800], +I[8.08, true, Welt-8], +I[80]]\n";

        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    void testTableSourceReadAsByteArray() {
        TableEnvironment tEnv = TableEnvironment.create(batchSettings);

        tEnv.executeSql(
                "CREATE TABLE hTable ("
                        + " family2 ROW<col1 BYTES, col2 BYTES>,"
                        + " rowkey INT"
                        + // no primary key syntax
                        ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_1
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");
        tEnv.registerFunction("toUTF8", new ToUTF8());
        tEnv.registerFunction("toLong", new ToLong());

        Table table =
                tEnv.sqlQuery(
                        "SELECT "
                                + "  toUTF8(h.family2.col1), "
                                + "  toLong(h.family2.col2) "
                                + "FROM hTable AS h");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[Hello-1, 100]\n"
                        + "+I[Hello-2, 200]\n"
                        + "+I[Hello-3, 300]\n"
                        + "+I[null, 400]\n"
                        + "+I[Hello-5, 500]\n"
                        + "+I[Hello-6, 600]\n"
                        + "+I[Hello-7, 700]\n"
                        + "+I[null, 800]\n";

        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    void testTableSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        // register HBase table testTable1 which contains test data
        String table1DDL = createHBaseTableDDL(TEST_TABLE_1, false);
        tEnv.executeSql(table1DDL);

        String table2DDL = createHBaseTableDDL(TEST_TABLE_2, false);
        tEnv.executeSql(table2DDL);

        String query =
                "INSERT INTO "
                        + TEST_TABLE_2
                        + " SELECT"
                        + " rowkey,"
                        + " family1,"
                        + " family2,"
                        + " family3"
                        + " FROM "
                        + TEST_TABLE_1;

        TableResult tableResult = tEnv.executeSql(query);

        // wait to finish
        tableResult.await();

        assertThat(tableResult.collect().next().getKind()).isEqualTo(RowKind.INSERT);

        // start a batch scan job to verify contents in HBase table
        TableEnvironment batchEnv = TableEnvironment.create(batchSettings);
        batchEnv.executeSql(table2DDL);

        List<String> expected = new ArrayList<>();
        expected.add("+I[1, 10, Hello-1, 100, 1.01, false, Welt-1]\n");
        expected.add("+I[2, 20, Hello-2, 200, 2.02, true, Welt-2]\n");
        expected.add("+I[3, 30, Hello-3, 300, 3.03, false, Welt-3]\n");
        expected.add("+I[4, 40, null, 400, 4.04, true, Welt-4]\n");
        expected.add("+I[5, 50, Hello-5, 500, 5.05, false, Welt-5]\n");
        expected.add("+I[6, 60, Hello-6, 600, 6.06, true, Welt-6]\n");
        expected.add("+I[7, 70, Hello-7, 700, 7.07, false, Welt-7]\n");
        expected.add("+I[8, 80, null, 800, 8.08, true, Welt-8]\n");

        Table countTable =
                batchEnv.sqlQuery("SELECT COUNT(h.rowkey) FROM " + TEST_TABLE_2 + " AS h");

        assertThat(countTable.execute().collect().next().getField(0))
                .isEqualTo(new Long(expected.size()));

        Table table =
                batchEnv.sqlQuery(
                        "SELECT "
                                + "  h.rowkey, "
                                + "  h.family1.col1, "
                                + "  h.family2.col1, "
                                + "  h.family2.col2, "
                                + "  h.family3.col1, "
                                + "  h.family3.col2, "
                                + "  h.family3.col3 "
                                + "FROM "
                                + TEST_TABLE_2
                                + " AS h");

        TableResult tableResult2 = table.execute();

        List<Row> results = CollectionUtil.iteratorToList(tableResult2.collect());

        TestBaseUtils.compareResultAsText(results, String.join("", expected));
    }

    @Test
    void testTableSinkWithChangelog() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 1, Row.of("Hello1")),
                                Row.ofKind(RowKind.DELETE, 1, Row.of("Hello2")),
                                Row.ofKind(RowKind.INSERT, 2, Row.of("Hello1")),
                                Row.ofKind(RowKind.INSERT, 2, Row.of("Hello2")),
                                Row.ofKind(RowKind.INSERT, 2, Row.of("Hello3")),
                                Row.ofKind(RowKind.DELETE, 2, Row.of("Hello3")),
                                Row.ofKind(RowKind.INSERT, 1, Row.of("Hello3"))));
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " rowkey INT,"
                        + " family1 ROW<name STRING>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '"
                        + dataId
                        + "',"
                        + " 'changelog-mode'='I,UA,UB,D'"
                        + ")");

        // register HBase table for sink
        tEnv.executeSql(
                "CREATE TABLE sink_table ("
                        + " rowkey INT,"
                        + " family1 ROW<name STRING>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_4
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table").await();

        TableResult result = tEnv.executeSql("SELECT * FROM sink_table");

        List<Row> actual = CollectionUtil.iteratorToList(result.collect());
        assertThat(actual).isEqualTo(Collections.singletonList(Row.of(1, Row.of("Hello3"))));
    }

    @Test
    void testTableSinkWithTimestampMetadata() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        tEnv.executeSql(
                "CREATE TABLE hTableForSink ("
                        + " rowkey INT PRIMARY KEY NOT ENFORCED,"
                        + " family1 ROW<col1 INT>,"
                        + " version TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp'"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_5
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        String insert =
                "INSERT INTO hTableForSink VALUES"
                        + "(1, ROW(1), TO_TIMESTAMP_LTZ(1696767943270, 3)),"
                        + "(2, ROW(2), TO_TIMESTAMP_LTZ(1696767943270, 3)),"
                        + "(3, ROW(3), TO_TIMESTAMP_LTZ(1696767943270, 3)),"
                        + "(1, ROW(10), TO_TIMESTAMP_LTZ(1696767943269, 3)),"
                        + "(2, ROW(20), TO_TIMESTAMP_LTZ(1696767943271, 3))";
        tEnv.executeSql(insert).await();

        tEnv.executeSql(
                "CREATE TABLE hTableForQuery ("
                        + " rowkey INT PRIMARY KEY NOT ENFORCED,"
                        + " family1 ROW<col1 INT>"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_5
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");
        TableResult result = tEnv.executeSql("SELECT rowkey, family1.col1 FROM hTableForQuery");
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        String expected = "+I[1, 1]\n+I[2, 20]\n+I[3, 3]\n";

        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    void testTableSinkWithTTLMetadata() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        tEnv.executeSql(
                "CREATE TABLE hTableForSink ("
                        + " rowkey INT PRIMARY KEY NOT ENFORCED,"
                        + " family1 ROW<col1 INT>,"
                        + " ttl BIGINT NOT NULL METADATA FROM 'ttl'"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_6
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        String insert =
                "INSERT INTO hTableForSink VALUES"
                        + "(1, ROW(1), 2000),"
                        + "(2, ROW(2), 9000),"
                        + "(3, ROW(3), 5000)";
        tEnv.executeSql(insert).await();

        tEnv.executeSql(
                "CREATE TABLE hTableForQuery ("
                        + " rowkey INT PRIMARY KEY NOT ENFORCED,"
                        + " family1 ROW<col1 INT>"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'table-name' = '"
                        + TEST_TABLE_6
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");
        String query = "SELECT rowkey, family1.col1 FROM hTableForQuery";

        TableResult firstResult = tEnv.executeSql(query);
        List<Row> firstResults = CollectionUtil.iteratorToList(firstResult.collect());
        String firstExpected = "+I[1, 1]\n+I[2, 2]\n+I[3, 3]\n";
        TestBaseUtils.compareResultAsText(firstResults, firstExpected);

        TimeUnit.SECONDS.sleep(3);

        TableResult secondResult = tEnv.executeSql(query);
        List<Row> secondResults = CollectionUtil.iteratorToList(secondResult.collect());
        String secondExpected = "+I[2, 2]\n+I[3, 3]\n";
        TestBaseUtils.compareResultAsText(secondResults, secondExpected);

        TimeUnit.SECONDS.sleep(3);

        TableResult thirdResult = tEnv.executeSql(query);
        List<Row> thirdResults = CollectionUtil.iteratorToList(thirdResult.collect());
        String thirdExpected = "+I[2, 2]";
        TestBaseUtils.compareResultAsText(thirdResults, thirdExpected);

        TimeUnit.SECONDS.sleep(4);

        TableResult lastResult = tEnv.executeSql(query);
        List<Row> lastResults = CollectionUtil.iteratorToList(lastResult.collect());
        assertThat(lastResults).isEmpty();
    }

    @Test
    void testTableSourceSinkWithDDL() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        // register HBase table testTable1 which contains test data
        String table1DDL = createHBaseTableDDL(TEST_TABLE_1, true);
        tEnv.executeSql(table1DDL);

        // register HBase table which is empty
        String table3DDL = createHBaseTableDDL(TEST_TABLE_3, true);
        tEnv.executeSql(table3DDL);

        String insertStatement =
                "INSERT INTO "
                        + TEST_TABLE_3
                        + " SELECT rowkey,"
                        + " family1,"
                        + " family2,"
                        + " family3,"
                        + " family4"
                        + " from "
                        + TEST_TABLE_1;

        TableResult tableResult = tEnv.executeSql(insertStatement);

        // wait to finish
        tableResult.await();

        assertThat(tableResult.collect().next().getKind()).isEqualTo(RowKind.INSERT);

        // start a batch scan job to verify contents in HBase table
        TableEnvironment batchEnv = TableEnvironment.create(batchSettings);
        batchEnv.executeSql(table3DDL);

        List<String> expected = new ArrayList<>();
        expected.add(
                "+I[1, 10, Hello-1, 100, 1.01, false, Welt-1, 2019-08-18T19:00, 2019-08-18, 19:00, 12345678.0001]");
        expected.add(
                "+I[2, 20, Hello-2, 200, 2.02, true, Welt-2, 2019-08-18T19:01, 2019-08-18, 19:01, 12345678.0002]");
        expected.add(
                "+I[3, 30, Hello-3, 300, 3.03, false, Welt-3, 2019-08-18T19:02, 2019-08-18, 19:02, 12345678.0003]");
        expected.add(
                "+I[4, 40, null, 400, 4.04, true, Welt-4, 2019-08-18T19:03, 2019-08-18, 19:03, 12345678.0004]");
        expected.add(
                "+I[5, 50, Hello-5, 500, 5.05, false, Welt-5, 2019-08-19T19:10, 2019-08-19, 19:10, 12345678.0005]");
        expected.add(
                "+I[6, 60, Hello-6, 600, 6.06, true, Welt-6, 2019-08-19T19:20, 2019-08-19, 19:20, 12345678.0006]");
        expected.add(
                "+I[7, 70, Hello-7, 700, 7.07, false, Welt-7, 2019-08-19T19:30, 2019-08-19, 19:30, 12345678.0007]");
        expected.add(
                "+I[8, 80, null, 800, 8.08, true, Welt-8, 2019-08-19T19:40, 2019-08-19, 19:40, 12345678.0008]");

        String query =
                "SELECT "
                        + "  h.rowkey, "
                        + "  h.family1.col1, "
                        + "  h.family2.col1, "
                        + "  h.family2.col2, "
                        + "  h.family3.col1, "
                        + "  h.family3.col2, "
                        + "  h.family3.col3, "
                        + "  h.family4.col1, "
                        + "  h.family4.col2, "
                        + "  h.family4.col3, "
                        + "  h.family4.col4 "
                        + " FROM "
                        + TEST_TABLE_3
                        + " AS h";

        TableResult tableResult3 = batchEnv.executeSql(query);

        List<String> result =
                StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(
                                        tableResult3.collect(), Spliterator.ORDERED),
                                false)
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testHBaseLookupTableSource(Caching caching) {
        verifyHBaseLookupJoin(caching, false);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testHBaseAsyncLookupTableSource(Caching caching) {
        verifyHBaseLookupJoin(caching, true);
    }

    @Test
    void testTableInputFormatOpenClose() throws IOException {
        HBaseTableSchema tableSchema = new HBaseTableSchema();
        tableSchema.addColumn(FAMILY1, F1COL1, byte[].class);
        AbstractTableInputFormat<?> inputFormat =
                new HBaseRowDataInputFormat(getConf(), TEST_TABLE_1, tableSchema, "null");
        inputFormat.open(inputFormat.createInputSplits(1)[0]);
        assertThat(inputFormat.getConnection()).isNotNull();
        assertThat(inputFormat.getConnection().getTable(TableName.valueOf(TEST_TABLE_1)))
                .isNotNull();

        inputFormat.close();
        assertThat(inputFormat.getConnection()).isNull();
    }

    @Test
    void testTableInputFormatTableExistence() throws IOException {
        HBaseTableSchema tableSchema = new HBaseTableSchema();
        tableSchema.addColumn(FAMILY1, F1COL1, byte[].class);
        AbstractTableInputFormat<?> inputFormat =
                new HBaseRowDataInputFormat(getConf(), TEST_NOT_EXISTS_TABLE, tableSchema, "null");

        assertThatThrownBy(() -> inputFormat.createInputSplits(1))
                .isExactlyInstanceOf(TableNotFoundException.class);

        inputFormat.close();
        assertThat(inputFormat.getConnection()).isNull();
    }

    @Test
    void testHBaseSinkFunctionTableExistence() throws Exception {
        org.apache.hadoop.conf.Configuration hbaseConf =
                HBaseConfigurationUtil.getHBaseConfiguration();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, getZookeeperQuorum());
        hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");

        HBaseTableSchema tableSchema = new HBaseTableSchema();
        tableSchema.addColumn(FAMILY1, F1COL1, byte[].class);

        HBaseSinkFunction<RowData> sinkFunction =
                new HBaseSinkFunction<>(
                        TEST_NOT_EXISTS_TABLE,
                        hbaseConf,
                        new RowDataToMutationConverter(
                                tableSchema,
                                tableSchema.convertToDataType(),
                                Collections.emptyList(),
                                "null",
                                false),
                        2 * 1024 * 1024,
                        1000,
                        1000);

        assertThatThrownBy(() -> sinkFunction.open(new Configuration()))
                .getRootCause()
                .isExactlyInstanceOf(TableNotFoundException.class);

        sinkFunction.close();
    }

    private void verifyHBaseLookupJoin(Caching caching, boolean async) {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        String cacheOptions = "";
        if (caching == Caching.ENABLE_CACHE) {
            cacheOptions =
                    ","
                            + String.join(
                                    ",",
                                    Arrays.asList(
                                            "'lookup.cache' = 'PARTIAL'",
                                            "'lookup.partial-cache.max-rows' = '1000'",
                                            "'lookup.partial-cache.expire-after-write' = '10min'"));
        }

        tEnv.executeSql(
                "CREATE TABLE "
                        + TEST_TABLE_1
                        + " ("
                        + " family1 ROW<col1 INT>,"
                        + " family2 ROW<col1 STRING, col2 BIGINT>,"
                        + " family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,"
                        + " rowkey INT,"
                        + " family4 ROW<col1 TIMESTAMP(3), col2 DATE, col3 TIME(3), col4 DECIMAL(12, 4)>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'hbase-2.2',"
                        + " 'lookup.async' = '"
                        + async
                        + "',"
                        + " 'table-name' = '"
                        + TEST_TABLE_1
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + cacheOptions
                        + ")");

        // prepare a source table
        String srcTableName = "src";
        DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(testTypeInfo);
        Table in = tEnv.fromDataStream(srcDs, $("a"), $("b"), $("c"), $("proc").proctime());
        tEnv.createTemporaryView(srcTableName, in);

        // perform a temporal table join query
        String dimJoinQuery =
                "SELECT"
                        + " a,"
                        + " b,"
                        + " h.family1.col1,"
                        + " h.family2.col1,"
                        + " h.family2.col2,"
                        + " h.family3.col1,"
                        + " h.family3.col2,"
                        + " h.family3.col3,"
                        + " h.family4.col1,"
                        + " h.family4.col2,"
                        + " h.family4.col3,"
                        + " h.family4.col4 "
                        + " FROM src JOIN "
                        + TEST_TABLE_1
                        + " FOR SYSTEM_TIME AS OF src.proc as h ON src.a = h.rowkey";
        Iterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> result =
                StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(collected, Spliterator.ORDERED),
                                false)
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add(
                "+I[1, 1, 10, Hello-1, 100, 1.01, false, Welt-1, 2019-08-18T19:00, 2019-08-18, 19:00, 12345678.0001]");
        expected.add(
                "+I[1, 1, 10, Hello-1, 100, 1.01, false, Welt-1, 2019-08-18T19:00, 2019-08-18, 19:00, 12345678.0001]");
        expected.add(
                "+I[2, 2, 20, Hello-2, 200, 2.02, true, Welt-2, 2019-08-18T19:01, 2019-08-18, 19:01, 12345678.0002]");
        expected.add(
                "+I[3, 2, 30, Hello-3, 300, 3.03, false, Welt-3, 2019-08-18T19:02, 2019-08-18, 19:02, 12345678.0003]");
        expected.add(
                "+I[3, 3, 30, Hello-3, 300, 3.03, false, Welt-3, 2019-08-18T19:02, 2019-08-18, 19:02, 12345678.0003]");

        assertThat(result).isEqualTo(expected);
    }

    // -------------------------------------------------------------------------------------
    // HBase lookup source tests
    // -------------------------------------------------------------------------------------

    // prepare a source collection.
    private static final List<Row> testData = new ArrayList<>();
    private static final RowTypeInfo testTypeInfo =
            new RowTypeInfo(
                    new TypeInformation[] {Types.INT, Types.LONG, Types.STRING},
                    new String[] {"a", "b", "c"});

    static {
        testData.add(Row.of(1, 1L, "Hi"));
        testData.add(Row.of(2, 2L, "Hello"));
        testData.add(Row.of(3, 2L, "Hello world"));
        testData.add(Row.of(3, 3L, "Hello world!"));
        testData.add(Row.of(1, 1L, "Hi")); // lookup one more time
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    // ------------------------------- Utilities -------------------------------------------------

    /** A {@link ScalarFunction} that maps byte arrays to UTF-8 strings. */
    public static class ToUTF8 extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(byte[] bytes) {
            return Bytes.toString(bytes);
        }
    }

    /** A {@link ScalarFunction} that maps byte array to longs. */
    public static class ToLong extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public long eval(byte[] bytes) {
            return Bytes.toLong(bytes);
        }
    }

    private String createHBaseTableDDL(String tableName, boolean testTimeAndDecimalTypes) {
        StringBuilder family4Statement = new StringBuilder();
        if (testTimeAndDecimalTypes) {
            family4Statement.append(", family4 ROW<col1 TIMESTAMP(3)");
            family4Statement.append(", col2 DATE");
            family4Statement.append(", col3 TIME(3)");
            family4Statement.append(", col4 DECIMAL(12, 4)");
            family4Statement.append("> \n");
        }

        return "CREATE TABLE "
                + tableName
                + "(\n"
                + "   rowkey INT,"
                + "   family1 ROW<col1 INT>,\n"
                + "   family2 ROW<col1 VARCHAR, col2 BIGINT>,\n"
                + "   family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 VARCHAR>"
                + family4Statement.toString()
                + ") WITH (\n"
                + "   'connector' = 'hbase-2.2',\n"
                + "   'table-name' = '"
                + tableName
                + "',\n"
                + "   'zookeeper.quorum' = '"
                + getZookeeperQuorum()
                + "',\n"
                + "   'zookeeper.znode.parent' = '/hbase' "
                + ")";
    }
}
