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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MergeTest extends TestBase {
    public MergeTest() {
        super(MergeTest.class);
    }

    @Test
    public void testMergeEmptyFast1() throws Exception {
        makeTestData1();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty-FAST", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
    }

    @Test
    public void testMergeEmptyFast2() throws Exception {
        makeTestData2();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty-FAST", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("a".getBytes(), regionInfoList.get(1).getStartKey());
    }

    @Test
    public void testMergeEmptyFast3() throws Exception {
        makeTestData3();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty-FAST", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("b".getBytes(), regionInfoList.get(1).getStartKey());
        assertArrayEquals("c".getBytes(), regionInfoList.get(2).getStartKey());
    }

    @Test
    public void testMergeEmptyFast4() throws Exception {
        makeTestData4();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty-FAST", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("c".getBytes(), regionInfoList.get(1).getStartKey());
    }

    @Test
    public void testMergeEmptyFast5() throws Exception {
        makeTestData5();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty-FAST", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(5, regionInfoList.size());
    }

    @Test
    public void testMergeEmptyFast6() throws Exception {
        makeTestData6();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty-FAST", "--force-proceed", "--max-iteration=4"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
    }

    @Test
    public void testMergeEmpty1() throws Exception {
        makeTestData1();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
    }

    @Test
    public void testMergeEmpty2() throws Exception {
        makeTestData2();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
    }

    @Test
    public void testMergeEmpty3() throws Exception {
        makeTestData3();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
    }

    @Test
    public void testMergeEmpty4() throws Exception {
        makeTestData4();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
    }

    @Test
    public void testMergeEmpty5() throws Exception {
        makeTestData5();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "emptY", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("b".getBytes(), regionInfoList.get(1).getStartKey());
    }

    @Test
    public void testMergeEmpty6() throws Exception {
        makeTestData6();

        List<HRegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName, "empty", "--force-proceed", "--max-iteration=4"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
    }

    private void makeTestData1() throws Exception {
        List<HRegionInfo> regionInfoList;// split table to 3 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
    }

    private void makeTestData2() throws Exception {
        List<HRegionInfo> regionInfoList;// split table to 4 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(4, regionInfoList.size());

        // put data to the first region
        try (HTableInterface table = hConnection.getTable(tableName)) {
            Put put = new Put("1".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    private void makeTestData3() throws Exception {
        List<HRegionInfo> regionInfoList;// split table to 4 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(4, regionInfoList.size());

        // put data to the second region
        try (HTableInterface table = hConnection.getTable(tableName)) {
            Put put = new Put("b".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    private void makeTestData4() throws Exception {
        List<HRegionInfo> regionInfoList;// split table to 4 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(4, regionInfoList.size());

        // put data to the last region
        try (HTableInterface table = hConnection.getTable(tableName)) {
            Put put = new Put("c".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    private void makeTestData5() throws Exception {
        List<HRegionInfo> regionInfoList;// split table to 5 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        splitTable("d".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(5, regionInfoList.size());

        // put data to the second and forth region
        try (HTableInterface table = hConnection.getTable(tableName)) {
            Put put;
            put = new Put("a".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
            put = new Put("c".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    private void makeTestData6() throws Exception {
        List<HRegionInfo> regionInfoList;// split table
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        splitTable("d".getBytes());
        splitTable("e".getBytes());
        splitTable("f".getBytes());
        splitTable("g".getBytes());
        splitTable("h".getBytes());
        splitTable("i".getBytes());
        splitTable("j".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(11, regionInfoList.size());
    }
}
