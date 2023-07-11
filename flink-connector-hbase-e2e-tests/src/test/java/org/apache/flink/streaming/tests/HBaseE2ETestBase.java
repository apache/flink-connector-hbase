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

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.hamcrest.MatcherAssert.assertThat;
import static org.testcontainers.shaded.org.hamcrest.Matchers.allOf;
import static org.testcontainers.shaded.org.hamcrest.Matchers.containsInAnyOrder;
import static org.testcontainers.shaded.org.hamcrest.Matchers.containsString;

/** End to end HBase connector tests. */
@ExtendWith(TestLoggerExtension.class)
abstract class HBaseE2ETestBase {

    private static final String HBASE_E2E_SQL = "hbase_e2e.sql";
    private static final Path HADOOP_CP = ResourceTestUtils.getResource(".*hadoop.classpath");

    private final String hbaseVersion;
    private final String connectorVersion;
    private final Path connectorJar;
    private final List<Path> hadoopCpJars;
    private final Network network;

    HBaseE2ETestBase(String hbaseVersion) throws IOException {
        this.hbaseVersion = hbaseVersion;
        connectorVersion = "hbase-" + hbaseVersion.substring(0, hbaseVersion.lastIndexOf('.'));
        connectorJar = ResourceTestUtils.getResource("sql-" + connectorVersion + ".jar");
        hadoopCpJars = collectHadoopCpJars();
        network = Network.newNetwork();
    }

    private HBaseContainer hbase;
    private FlinkContainers flink;

    @BeforeEach
    void start() throws Exception {
        hbase = new HBaseContainer(hbaseVersion).withNetwork(network).withNetworkAliases("hbase");
        flink =
                FlinkContainers.builder()
                        .withTestcontainersSettings(
                                TestcontainersSettings.builder()
                                        .network(network)
                                        .dependsOn(hbase)
                                        .build())
                        .build();

        hbase.start();
        flink.start();
    }

    @AfterEach
    void stop() {
        flink.stop();
        hbase.stop();
    }

    @Test
    void test() throws Exception {
        hbase.createTable("source", "family1", "family2");
        hbase.createTable("sink", "family1", "family2");

        hbase.putData("source", "row1", "family1", "f1c1", "v1");
        hbase.putData("source", "row1", "family2", "f2c1", "v2");
        hbase.putData("source", "row1", "family2", "f2c2", "v3");
        hbase.putData("source", "row2", "family1", "f1c1", "v4");
        hbase.putData("source", "row2", "family2", "f2c1", "v5");
        hbase.putData("source", "row2", "family2", "f2c2", "v6");

        SQLJobSubmission jobSubmission = initSqlJobSubmission();
        flink.submitSQLJob(jobSubmission);
        List<String> valueLines = getSinkResult();

        assertEquals(6, valueLines.size());

        assertThat(
                valueLines,
                containsInAnyOrder(
                        allOf(
                                containsString("row1"),
                                containsString("family1"),
                                containsString("f1c1"),
                                containsString("value1")),
                        allOf(
                                containsString("row1"),
                                containsString("family2"),
                                containsString("f2c1"),
                                containsString("v2")),
                        allOf(
                                containsString("row1"),
                                containsString("family2"),
                                containsString("f2c2"),
                                containsString("v3")),
                        allOf(
                                containsString("row2"),
                                containsString("family1"),
                                containsString("f1c1"),
                                containsString("value4")),
                        allOf(
                                containsString("row2"),
                                containsString("family2"),
                                containsString("f2c1"),
                                containsString("v5")),
                        allOf(
                                containsString("row2"),
                                containsString("family2"),
                                containsString("f2c2"),
                                containsString("v6"))));
    }

    private List<Path> collectHadoopCpJars() throws IOException {
        // Prepare all hadoop jars to mock HADOOP_CLASSPATH, use hadoop.classpath which contains all
        // hadoop jars
        File hadoopClasspathFile = new File(HADOOP_CP.toAbsolutePath().toString());

        if (!hadoopClasspathFile.exists()) {
            throw new FileNotFoundException(
                    "File that contains hadoop classpath " + HADOOP_CP + " does not exist.");
        }

        String classPathContent = FileUtils.readFileUtf8(hadoopClasspathFile);
        return Arrays.stream(classPathContent.split(":"))
                .map(Paths::get)
                .collect(Collectors.toList());
    }

    private SQLJobSubmission initSqlJobSubmission() throws IOException {
        List<String> sqlLines = loadSqlStatements();
        return new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                .addJar(connectorJar)
                .addJars(hadoopCpJars)
                .build();
    }

    private List<String> getSinkResult() throws Exception {
        Container.ExecResult res = hbase.scanTable("sink");
        assertEquals(0, res.getExitCode());

        return Arrays.stream(res.getStdout().split("\n"))
                .filter(line -> line.contains("value="))
                .collect(Collectors.toList());
    }

    private List<String> loadSqlStatements() throws IOException {
        try (InputStream is =
                HBaseE2ETestBase.class.getClassLoader().getResourceAsStream(HBASE_E2E_SQL)) {
            if (is == null) {
                throw new FileNotFoundException(HBASE_E2E_SQL);
            }

            List<String> lines = IOUtils.readLines(is, StandardCharsets.UTF_8);

            return lines.stream()
                    .map(line -> line.replace("$HBASE_CONNECTOR", connectorVersion))
                    .collect(Collectors.toList());
        }
    }
}
