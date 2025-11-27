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

import org.apache.flink.connector.hbase2.util.HBaseTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hbase.HBaseIOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases that simulate failures/timeouts for HBase. */
class HBaseConnectorFailureITCase extends HBaseTestBase {

    @AfterEach
    public void afterEach() throws IOException {
        if (hbaseTestingUtility.getMiniHBaseCluster().getNumLiveRegionServers() == 0) {
            hbaseTestingUtility.getMiniHBaseCluster().startRegionServer();
        }
    }

    /** This should fail on timeout because `fail-on-timeout` is set to true. */
    @Test
    void testTableSinkWithStoppedRegionServerShouldFail() throws HBaseIOException {
        hbaseTestingUtility.getMiniHBaseCluster().stopRegionServer(0);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Collections.singletonList(Row.ofKind(RowKind.INSERT, 1, Row.of("Hello1"))));
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
                        + " 'connector' = 'hbase-2.6',"
                        + " 'sink.request-timeout' = '100',"
                        + " 'sink.fail-on-timeout' = 'true',"
                        + " 'table-name' = '"
                        + TEST_TABLE_4
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table")
                                        .await())
                .isExactlyInstanceOf(ExecutionException.class)
                .hasCauseExactlyInstanceOf(TableException.class)
                .hasRootCauseExactlyInstanceOf(TimeoutException.class);
    }

    /** This should not fail on timeout because `fail-on-timeout` is set to false. */
    @Test
    void testTableSinkWithStoppedRegionServerShouldNotFail() throws Exception {
        hbaseTestingUtility.getMiniHBaseCluster().stopRegionServer(0);

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
                        + " 'connector' = 'hbase-2.6',"
                        + " 'sink.request-timeout' = '100',"
                        + " 'sink.fail-on-timeout' = 'false',"
                        + " 'table-name' = '"
                        + TEST_TABLE_4
                        + "',"
                        + " 'zookeeper.quorum' = '"
                        + getZookeeperQuorum()
                        + "'"
                        + ")");

        // Restart region server to ensure that requests will be tried again after timeout
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.schedule(
                () -> {
                    try {
                        hbaseTestingUtility.getMiniHBaseCluster().startRegionServer();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                3,
                TimeUnit.SECONDS);

        tEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table").await();

        TableResult result = tEnv.executeSql("SELECT * FROM sink_table");

        List<Row> actual = CollectionUtil.iteratorToList(result.collect());
        assertThat(actual).isEqualTo(Collections.singletonList(Row.of(1, Row.of("Hello3"))));
    }
}
