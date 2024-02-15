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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MCTest extends TestBase {
    private Table table = null;

    public MCTest() {
        super(MCTest.class);
    }

    private void putData(Table table, byte[] rowkey) throws IOException {
        Put put = new Put(rowkey);
        put.addColumn(TEST_TABLE_CF, "c1".getBytes(), rowkey);
        table.put(put);
    }

    private void putData2(Table table, byte[] rowkey) throws IOException {
        Put put = new Put(rowkey);
        put.addColumn(TEST_TABLE_CF, "c1".getBytes(), rowkey);
        put.addColumn(TEST_TABLE_CF2, "c1".getBytes(), rowkey);
        table.put(put);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        table = getTable(tableName);
    }

    @After
    public void tearDown() throws Exception {
        if (table != null) {
            table.close();
            table = null;
        }
        super.tearDown();
    }

    @Test
    public void testMC() throws Exception {
        // move a region to the first RS
        ArrayList<ServerName> serverNameList = getServerNameList();
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        RegionInfo regionInfo = regionInfoList.get(0);
        ServerName serverName = serverNameList.get(0);
        move(regionInfo, serverName);

        // make 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo, serverName).getStoreFileCount());

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // should be 1 store file
        assertEquals(1, getRegionMetrics(regionInfo, serverName).getStoreFileCount());
    }

    @Test
    public void testMC_MultipleTables() throws Exception {
        createAdditionalTable(tableName + "2");

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // 1 table should be compacted
        assertEquals(1, command.getMcCounter());
    }

    @Test
    public void testMC_CF() throws Exception {
        // add CF
        ColumnFamilyDescriptor cd = ColumnFamilyDescriptorBuilder.newBuilder(TEST_TABLE_CF2).build();
        admin.addColumnFamily(tableName, cd);

        // move a region to the first RS
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        RegionInfo regionInfo = regionInfoList.get(0);
        ServerName serverName = serverNameList.get(0);
        move(regionInfo, serverName);

        // make 2 + 2 store files
        putData2(table, "a".getBytes());
        admin.flush(tableName);
        putData2(table, "b".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2 + 2, getRegionMetrics(regionInfo, serverName).getStoreFileCount());

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--cf=d", "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // should be 2 + 1 store files
        assertEquals(2 + 1, getRegionMetrics(regionInfo, serverName).getStoreFileCount());
    }

    @Test
    public void testMC_InvalidCF() throws Exception {
        // move a region to the first RS
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        RegionInfo regionInfo = regionInfoList.get(0);
        ServerName serverName = serverNameList.get(0);
        move(regionInfo, serverName);

        // make 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo, serverName).getStoreFileCount());

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--cf=e", "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // should be 1 store file
        assertEquals(2, getRegionMetrics(regionInfo, serverName).getStoreFileCount());
    }

    @Test
    public void testMC_MultipleRSs() throws Exception {
        // move each RS has one region
        splitTable("c".getBytes());
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        RegionInfo regionInfo1 = regionInfoList.get(0);
        RegionInfo regionInfo2 = regionInfoList.get(1);
        ServerName serverName1 = serverNameList.get(0);
        ServerName serverName2 = serverNameList.get(1);
        move(regionInfo1, serverName1);
        move(regionInfo2, serverName2);

        // make 2 + 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        putData(table, "c".getBytes());
        admin.flush(tableName);
        putData(table, "d".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(2, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // should be 1 store file
        assertEquals(1, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(1, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());
    }

    @Test
    public void testMC_RS() throws Exception {
        // move each RS has one region
        splitTable("c".getBytes());
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        RegionInfo regionInfo1 = regionInfoList.get(0);
        RegionInfo regionInfo2 = regionInfoList.get(1);
        ServerName serverName1 = serverNameList.get(0);
        ServerName serverName2 = serverNameList.get(1);
        move(regionInfo1, serverName1);
        move(regionInfo2, serverName2);

        // make 2 + 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        putData(table, "c".getBytes());
        admin.flush(tableName);
        putData(table, "d".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(2, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());

        // run MC
        String regex = serverName1.getHostname() + "," + serverName1.getPort() + ".*";
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--rs=" + regex, "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // should be 1 store file
        assertEquals(1, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(2, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());
    }

    @Test
    public void testMC_Locality() throws Exception {
        // fixme Data locality is not correct in HBaseTestingUtility
        if (miniCluster) return;

        // move regions for each RS to have one region
        splitTable("c".getBytes());
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        RegionInfo regionInfo1 = regionInfoList.get(0);
        RegionInfo regionInfo2 = regionInfoList.get(1);
        ServerName serverName1 = serverNameList.get(0);
        ServerName serverName2 = serverNameList.get(1);
        move(regionInfo1, serverName1);
        move(regionInfo2, serverName1);

        // make 2 + 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        putData(table, "c".getBytes());
        admin.flush(tableName);
        putData(table, "d".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(1.0, getRegionMetrics(regionInfo2, serverName1).getDataLocality(), 0.0);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(1.0, getRegionMetrics(regionInfo2, serverName1).getDataLocality(), 0.0);

        // Data locality of regionInfo1 is 100%, data locality of regionInfo2 is 0%
        move(regionInfo2, serverName2);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(2, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--locality=100", "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();

        // should not be major-compacted
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(1, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());
    }

    @Test
    public void testMC_LocalityMultipleRSs() throws Exception {
        // fixme Data locality is not correct in HBaseTestingUtility
        if (miniCluster) return;

        // move regions for each RS to have one region
        splitTable("c".getBytes());
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        RegionInfo regionInfo1 = regionInfoList.get(0);
        RegionInfo regionInfo2 = regionInfoList.get(1);
        ServerName serverName1 = serverNameList.get(0);
        ServerName serverName2 = serverNameList.get(1);
        move(regionInfo1, serverName1);
        move(regionInfo2, serverName2);

        // make 2 + 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        putData(table, "c".getBytes());
        admin.flush(tableName);
        putData(table, "d".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName1).getStoreFileCount());
        assertEquals(0.0, getRegionMetrics(regionInfo2, serverName1).getDataLocality(), 0.0);
        assertEquals(2, getRegionMetrics(regionInfo2, serverName2).getStoreFileCount());
        assertEquals(0.0, getRegionMetrics(regionInfo2, serverName1).getDataLocality(), 0.0);

        // Data locality of regionInfo1 is 0%, data locality of regionInfo2 is 0%
        move(regionInfo1, serverName2);
        move(regionInfo2, serverName1);
        assertEquals(2, getRegionMetrics(regionInfo1, serverName2).getStoreFileCount());
        assertEquals(2, getRegionMetrics(regionInfo2, serverName1).getStoreFileCount());

        // run MC
        String regex;
        if (miniCluster) regex = serverName1.getHostname() + "," + serverName1.getPort() + ".*";
        else regex = serverName1.getHostname() + ".*";
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--locality=100", "--rs=" + regex,
                "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();

        assertEquals(1, getRegionMetrics(regionInfo2, serverName1).getStoreFileCount());
        assertEquals(2, getRegionMetrics(regionInfo1, serverName2).getStoreFileCount());
    }

    @Test
    public void testMC_WaitUntilFinishing() throws Exception {
        // move a region to the first RS
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        RegionInfo regionInfo = regionInfoList.get(0);
        ServerName serverName = serverNameList.get(0);
        move(regionInfo, serverName);

        // make 2 store files
        putData(table, "a".getBytes());
        admin.flush(tableName);
        putData(table, "b".getBytes());
        admin.flush(tableName);
        Thread.sleep(3000);
        assertEquals(2, getRegionMetrics(regionInfo, serverName).getStoreFileCount());

        // run MC
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();
        assertRegionName(command);

        // should be 1 store file
        assertEquals(1, getRegionMetrics(regionInfo, serverName).getStoreFileCount());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void assertRegionName(MC command) throws IOException {
        if (command.isTableLevel()) return;

        Set<byte[]> targets = command.getTargetRegions();
        for (byte[] region : targets) {
            RegionInfo.parseRegionName(region);
        }
    }
}
