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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class MCTest extends TestBase {
    private HTableInterface table = null;

    public MCTest() {
        super(MCTest.class);
    }

    private void putData(HTableInterface table, byte[] rowkey) throws IOException {
        Put put = new Put(rowkey);
        put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), rowkey);
        table.put(put);
    }

    private void putData2(HTableInterface table, byte[] rowkey) throws IOException {
        Put put = new Put(rowkey);
        put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), rowkey);
        put.addColumn(TEST_TABLE_CF2.getBytes(), "c1".getBytes(), rowkey);
        table.put(put);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        table = hConnection.getTable(tableName);
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
    public void testMC_MultipleTables() throws Exception {
        createAdditionalTable(tableName + "2");

        // run MC
        String[] argsParam = {"zookeeper", tableName, "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();

        // 1 table should be compacted
        assertEquals(1, command.getMcCounter());
    }

    @Test
    public void testMC_Locality() throws Exception {
        // fixme Data locality is not correct in HBaseTestingUtility
        if (miniCluster) return;

        // move each RS has one region
        splitTable("c".getBytes());
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<HRegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        HRegionInfo regionInfo1 = regionInfoList.get(0);
        HRegionInfo regionInfo2 = regionInfoList.get(1);
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
        assertEquals(2, getRegionLoad(regionInfo1, serverName1).getStorefiles());
        assertEquals(2, getRegionLoad(regionInfo2, serverName1).getStorefiles());

        // Data locality of regionInfo1 is 100%, data locality of regionInfo2 is 0%
        move(regionInfo2, serverName2);
        assertEquals(2, getRegionLoad(regionInfo1, serverName1).getStorefiles());
        assertEquals(2, getRegionLoad(regionInfo2, serverName2).getStorefiles());

        // run MC
        String[] argsParam = {"zookeeper", tableName, "--locality=100", "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();

        // should be 1 store file
        assertEquals(2, getRegionLoad(regionInfo1, serverName1).getStorefiles());
        assertEquals(1, getRegionLoad(regionInfo2, serverName2).getStorefiles());
    }

    @Test
    public void testMC_LocalityMultipleRSs() throws Exception {
        // fixme Data locality is not correct in HBaseTestingUtility
        if (miniCluster) return;

        // move each RS has one region
        splitTable("c".getBytes());
        ArrayList<ServerName> serverNameList = getServerNameList();
        assertTrue(serverNameList.size() >= 2);
        ArrayList<HRegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        HRegionInfo regionInfo1 = regionInfoList.get(0);
        HRegionInfo regionInfo2 = regionInfoList.get(1);
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
        assertEquals(2, getRegionLoad(regionInfo1, serverName1).getStorefiles());
        assertEquals(2, getRegionLoad(regionInfo2, serverName2).getStorefiles());

        // Data locality of regionInfo1 is 0%, data locality of regionInfo2 is 0%
        move(regionInfo1, serverName2);
        move(regionInfo2, serverName1);
        assertEquals(2, getRegionLoad(regionInfo1, serverName2).getStorefiles());
        assertEquals(2, getRegionLoad(regionInfo2, serverName1).getStorefiles());

        // run MC
        String regex;
        if (miniCluster) regex = serverName1.getHostname() + "," + serverName1.getPort() + ".*";
        else regex = serverName1.getHostname() + ".*";
        String[] argsParam = {"zookeeper", tableName, "--locality=100", "--rs=" + regex,
            "--force-proceed", "--wait", "--test"};
        Args args = new ManagerArgs(argsParam);
        MC command = new MC(admin, args);
        command.run();

        // should be 1 store file
        assertEquals(2, getRegionLoad(regionInfo1, serverName2).getStorefiles());
        assertEquals(1, getRegionLoad(regionInfo2, serverName1).getStorefiles());
    }
}
