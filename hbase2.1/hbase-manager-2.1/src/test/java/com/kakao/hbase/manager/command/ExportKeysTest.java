/*
 * Copyright 2015 Kakao Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kakao.hbase.manager.command;

import com.kakao.hbase.ManagerArgs;
import com.kakao.hbase.TestBase;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ExportKeysTest extends TestBase {
    public ExportKeysTest() {
        super(ExportKeysTest.class);
    }

    @Test
    public void testRun() throws Exception {
        String outputFile = "exportkeys_test.keys";
        try {
            byte[] splitPoint = "splitpoint".getBytes();

            splitTable(splitPoint);

            String[] argsParam = {"zookeeper", tableName.getNameAsString(), outputFile};
            Args args = new ManagerArgs(argsParam);
            assertEquals("zookeeper", args.getZookeeperQuorum());
            ExportKeys command = new ExportKeys(admin, args);
            waitForSplitting(2);
            command.run();

            int i = 0;
            List<Triple<String, String, String>> results = new ArrayList<>();
            for (String keys : Files.readAllLines(Paths.get(outputFile), Constant.CHARSET)) {
                i++;

                String[] split = keys.split(ExportKeys.DELIMITER);
                results.add(new ImmutableTriple<>(split[0], split[1], split[2]));
            }
            assertEquals(2, i);

            assertEquals(tableName.getNameAsString(), results.get(0).getLeft().trim());
            assertArrayEquals("".getBytes(), Bytes.toBytesBinary(results.get(0).getMiddle().trim()));
            assertArrayEquals(splitPoint, Bytes.toBytesBinary(results.get(0).getRight().trim()));
            assertEquals(tableName.getNameAsString(), results.get(1).getLeft().trim());
            assertArrayEquals(splitPoint, Bytes.toBytesBinary(results.get(1).getMiddle().trim()));
            assertArrayEquals("".getBytes(), Bytes.toBytesBinary(results.get(1).getRight().trim()));

            // split once more
            byte[] splitPoint2 = Bytes.toBytes(100L);

            splitTable(splitPoint2);

            command.run();

            i = 0;
            results.clear();
            for (String keys : Files.readAllLines(Paths.get(outputFile), Constant.CHARSET)) {
                i++;

                String[] split = keys.split(ExportKeys.DELIMITER);
                results.add(new ImmutableTriple<>(split[0], split[1], split[2]));
            }
            assertEquals(3, i);

            assertEquals(tableName.getNameAsString(), results.get(0).getLeft().trim());
            assertArrayEquals("".getBytes(), Bytes.toBytesBinary(results.get(0).getMiddle().trim()));
            assertArrayEquals(splitPoint2, Bytes.toBytesBinary(results.get(0).getRight().trim()));
            assertEquals(tableName.getNameAsString(), results.get(1).getLeft().trim());
            assertArrayEquals(splitPoint2, Bytes.toBytesBinary(results.get(1).getMiddle().trim()));
            assertArrayEquals(splitPoint, Bytes.toBytesBinary(results.get(1).getRight().trim()));
            assertEquals(tableName.getNameAsString(), results.get(2).getLeft().trim());
            assertArrayEquals(splitPoint, Bytes.toBytesBinary(results.get(2).getMiddle().trim()));
            assertArrayEquals("".getBytes(), Bytes.toBytesBinary(results.get(2).getRight().trim()));
        } finally {
            Files.delete(Paths.get(outputFile));
        }
    }

    @Test
    public void testRunOptimize() throws Exception {
        String outputFile = "exportkeys_test.keys";

        try {
            String splitPoint = "splitpoint";

            splitTable(splitPoint.getBytes());

            String[] argsParam = {"zookeeper", tableName.getNameAsString(), outputFile, "--optimize=1g"};
            Args args = new ManagerArgs(argsParam);
            assertEquals("zookeeper", args.getZookeeperQuorum());
            ExportKeys command = new ExportKeys(admin, args);

            waitForSplitting(2);
            command.run();

            List<Triple<String, String, String>> results = new ArrayList<>();
            for (String keys : Files.readAllLines(Paths.get(outputFile), Constant.CHARSET)) {

                String[] split = keys.split(ExportKeys.DELIMITER);
                results.add(new ImmutableTriple<>(split[0], split[1], split[2]));
            }
            assertEquals(0, results.size());
        } finally {
            Files.delete(Paths.get(outputFile));
        }
    }

    @Test
    public void testRegex() throws Exception {
        String outputFile = "exportkeys_test.keys";

        try {
            String splitPoint = "splitpoint";
            splitTable(splitPoint.getBytes());
            TableName tableName2 = createAdditionalTable(tableName + "2");
            splitTable(tableName2, splitPoint.getBytes());

            String tableNameRegex = tableName + ".*";
            String[] argsParam = {"zookeeper", tableNameRegex, outputFile};
            Args args = new ManagerArgs(argsParam);
            assertEquals("zookeeper", args.getZookeeperQuorum());
            ExportKeys command = new ExportKeys(admin, args);

            waitForSplitting(2);
            waitForSplitting(tableName2, 2);
            command.run();

            List<Triple<String, String, String>> results = new ArrayList<>();
            for (String keys : Files.readAllLines(Paths.get(outputFile), Constant.CHARSET)) {

                String[] split = keys.split(ExportKeys.DELIMITER);
                results.add(new ImmutableTriple<>(split[0], split[1], split[2]));
            }
            assertEquals(4, results.size());
        } finally {
            Files.delete(Paths.get(outputFile));
        }
    }

    @Test
    public void testRegexAll() throws Exception {
        if (miniCluster) {
            String outputFile = "exportkeys_test.keys";

            try {
                String splitPoint = "splitpoint";
                splitTable(splitPoint.getBytes());
                TableName tableName2 = createAdditionalTable(tableName + "2");
                splitTable(tableName2, splitPoint.getBytes());

                String[] argsParam = {"zookeeper", ".*", outputFile};
                Args args = new ManagerArgs(argsParam);
                assertEquals("zookeeper", args.getZookeeperQuorum());
                ExportKeys command = new ExportKeys(admin, args);

                waitForSplitting(2);
                waitForSplitting(tableName2, 2);
                command.run();

                List<Triple<String, String, String>> results = new ArrayList<>();
                for (String keys : Files.readAllLines(Paths.get(outputFile), Constant.CHARSET)) {

                    String[] split = keys.split(ExportKeys.DELIMITER);
                    results.add(new ImmutableTriple<>(split[0], split[1], split[2]));
                }
                assertEquals(4, results.size());
            } finally {
                Files.delete(Paths.get(outputFile));
            }
        }
    }

    @Test
    public void testInvalidTable() throws Exception {
        String[] argsParam = {"zookeeper"};
        Args args = new ManagerArgs(argsParam);
        assertEquals("zookeeper", args.getZookeeperQuorum());

        try {
            ExportKeys command = new ExportKeys(admin, args);
            command.run();
            fail();
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains(Args.INVALID_ARGUMENTS)) throw e;
        }
    }
}
