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
import com.kakao.hbase.common.util.Util;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class BalanceTest extends TestBase {
    public BalanceTest() {
        super(BalanceTest.class);
    }


    @Test
    public void testBalanceDefault() throws Exception {
        splitTable("a".getBytes());

        NavigableMap<RegionInfo, ServerName> regionLocations;
        List<Map.Entry<RegionInfo, ServerName>> hRegionInfoList;

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertEquals(2, regionLocations.size());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "default", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        command.run();

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertEquals(2, hRegionInfoList.size());
    }

    @Test
    public void testParseTableSet() throws Exception {
        String[] argsParam;
        Args args;
        Set<TableName> tableSet;

        createAdditionalTable(TestBase.tableName + "2");
        createAdditionalTable(TestBase.tableName + "22");
        createAdditionalTable(TestBase.tableName + "3");

        argsParam = new String[]{"zookeeper", ".*", "st", "--force-proceed", "--test"};
        args = new ManagerArgs(argsParam);
        tableSet = Util.parseTableSet(admin, args);
        for (TableName tableNameArg : tableSet) {
            assertTrue(tableNameArg.getNameAsString().startsWith(tableName.getNameAsString()));
        }
        Assert.assertEquals(4, tableSet.size());

        argsParam = new String[]{"zookeeper", tableName.getNameAsString(), "st", "--force-proceed"};
        args = new ManagerArgs(argsParam);
        tableSet = Util.parseTableSet(admin, args);
        Assert.assertEquals(1, tableSet.size());

        argsParam = new String[]{"zookeeper", tableName + ".*", "st", "--force-proceed"};
        args = new ManagerArgs(argsParam);
        tableSet = Util.parseTableSet(admin, args);
        Assert.assertEquals(4, tableSet.size());

        argsParam = new String[]{"zookeeper", tableName + "2.*", "st", "--force-proceed"};
        args = new ManagerArgs(argsParam);
        tableSet = Util.parseTableSet(admin, args);
        Assert.assertEquals(2, tableSet.size());

        argsParam = new String[]{"zookeeper", tableName + "2.*," + tableName + "3.*", "st", "--force-proceed"};
        args = new ManagerArgs(argsParam);
        tableSet = Util.parseTableSet(admin, args);
        Assert.assertEquals(3, tableSet.size());
    }

    @Test
    public void testBalanceAllTables() throws Exception {
        List<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;

        // create tables
        TableName tableName2 = createAdditionalTable(TestBase.tableName + "2");
        TableName tableName3 = createAdditionalTable(TestBase.tableName + "3");

        // move all regions to rs1
        serverNameList = getServerNameList();
        ServerName rs1 = serverNameList.get(0);
        regionInfoList = getRegionInfoList(tableName);
        regionInfoList.addAll(getRegionInfoList(tableName2));
        regionInfoList.addAll(getRegionInfoList(tableName3));
        for (RegionInfo hRegionInfo : regionInfoList) {
            move(hRegionInfo, rs1);
        }
        Assert.assertEquals(3, getRegionInfoList(rs1, tableName).size() + getRegionInfoList(rs1, tableName2).size() + getRegionInfoList(rs1, tableName3).size());

        String[] argsParam = {"zookeeper", ".*", "st", "--force-proceed", "--test"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        command.run();

        // check regions balanced
        Assert.assertNotEquals(3, getRegionInfoList(rs1, tableName).size() + getRegionInfoList(rs1, tableName2).size() + getRegionInfoList(rs1, tableName3).size());
    }

    @Test
    public void testBalanceByFactor() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());

        NavigableMap<RegionInfo, ServerName> regionLocations;
        List<Map.Entry<RegionInfo, ServerName>> hRegionInfoList;

        // set regions unbalanced
        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertEquals(4, regionLocations.size());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(2).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(3).getValue());

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "St", "--force-proceed", "--factor=ss"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        // balance
        command.run();

        // assert
        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Map<ServerName, Integer> serverCountMap = new HashMap<>();
        for (Map.Entry<RegionInfo, ServerName> entry : hRegionInfoList) {
            serverCountMap.merge(entry.getValue(), 1, Integer::sum);
        }
        int regionCount = 0;
        for (ServerName serverName : getServerNameList()) {
            List<RegionInfo> regionInfoList = getRegionInfoList(serverName, tableName);
            Assert.assertNotEquals(4, regionInfoList);
            regionCount += regionInfoList.size();
        }
        Assert.assertEquals(4, regionCount);
    }

    @Test
    public void testBalanceAsync() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());

        NavigableMap<RegionInfo, ServerName> regionLocations;
        List<Map.Entry<RegionInfo, ServerName>> hRegionInfoList;

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertEquals(4, regionLocations.size());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(2).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(3).getValue());

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "rr", "--force-proceed", "--move-async"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        command.run();

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertNotEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertNotEquals(hRegionInfoList.get(2).getValue(), hRegionInfoList.get(3).getValue());
    }
}
