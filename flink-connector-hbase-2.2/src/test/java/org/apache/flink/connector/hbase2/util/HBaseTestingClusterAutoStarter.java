/*
 * Copyright The Apache Software Foundation
 *
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

package org.apache.flink.connector.hbase2.util;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.commons.lang3.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.VersionUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * By using this class as the super class of a set of tests you will have a HBase testing cluster
 * available that is very suitable for writing tests for scanning and filtering against.
 */
public class HBaseTestingClusterAutoStarter {
    private static final Log LOG = LogFactory.getLog(HBaseTestingClusterAutoStarter.class);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final Range<String> HADOOP_VERSION_RANGE =
            Range.between("2.8.0", "3.0.3", VersionUtil::compareVersions);

    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static Admin admin = null;
    private static List<TableName> createdTables = new ArrayList<>();

    private static Configuration conf;

    protected static void createTable(
            TableName tableName, byte[][] columnFamilyName, byte[][] splitKeys) {
        assertThat(admin).as("HBaseAdmin is not initialized successfully.").isNotNull();
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (byte[] fam : columnFamilyName) {
            HColumnDescriptor colDef = new HColumnDescriptor(fam);
            desc.addFamily(colDef);
        }

        try {
            admin.createTable(desc, splitKeys);
            createdTables.add(tableName);
            assertThat(admin.tableExists(tableName)).as("Fail to create the table").isTrue();
        } catch (IOException e) {
            fail("Exception found while creating table", e);
        }
    }

    protected static Table openTable(TableName tableName) throws IOException {
        Table table = TEST_UTIL.getConnection().getTable(tableName);
        assertThat(admin.tableExists(tableName)).as("Fail to create the table").isTrue();
        return table;
    }

    private static void deleteTables() {
        if (admin != null) {
            for (TableName tableName : createdTables) {
                try {
                    if (admin.tableExists(tableName)) {
                        admin.disableTable(tableName);
                        admin.deleteTable(tableName);
                    }
                } catch (IOException e) {
                    fail("Exception found deleting the table", e);
                }
            }
        }
    }

    public static Configuration getConf() {
        return conf;
    }

    public static String getZookeeperQuorum() {
        return "localhost:" + TEST_UTIL.getZkCluster().getClientPort();
    }

    private static void initialize(Configuration c) {
        conf = HBaseConfiguration.create(c);
        // the default retry number is 15 in hbase-2.2, set 15 for test
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 15);
        try {
            admin = TEST_UTIL.getAdmin();
        } catch (MasterNotRunningException e) {
            fail("Master is not running", e);
        } catch (ZooKeeperConnectionException e) {
            fail("Cannot connect to ZooKeeper", e);
        } catch (IOException e) {
            fail("IOException", e);
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        // HBase 2.2.3 HBaseTestingUtility works with only a certain range of hadoop versions
        String hadoopVersion = System.getProperty("hadoop.version", "2.8.5");
        assumeThat(HADOOP_VERSION_RANGE.contains(hadoopVersion)).isTrue();
        TEST_UTIL.startMiniCluster(1);

        // https://issues.apache.org/jira/browse/HBASE-11711
        TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", -1);

        // Make sure the zookeeper quorum value contains the right port number (varies per run).
        LOG.info("Hbase minicluster client port: " + TEST_UTIL.getZkCluster().getClientPort());
        TEST_UTIL
                .getConfiguration()
                .set(
                        "hbase.zookeeper.quorum",
                        "localhost:" + TEST_UTIL.getZkCluster().getClientPort());

        initialize(TEST_UTIL.getConfiguration());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (conf == null) {
            LOG.info("Skipping Hbase tear down. It was never started");
            return;
        }
        LOG.info("HBase minicluster: Shutting down");
        deleteTables();
        TEST_UTIL.shutdownMiniCluster();
        LOG.info("HBase minicluster: Down");
    }
}
