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
import org.apache.hadoop.hbase.master.RegionPlan;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class BalanceRuleTest extends TestBase {
    public BalanceRuleTest() {
        super(BalanceRuleTest.class);
    }

    @Test
    public void testBalanceRD() throws Exception {
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

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "rD", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        // fixme
        command.run();

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertNotEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertNotEquals(hRegionInfoList.get(2).getValue(), hRegionInfoList.get(3).getValue());
    }

    @Test
    public void testBalanceST() throws Exception {
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

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "St", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        command.run();

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Map<ServerName, Integer> serverCountMap = new HashMap<>();
        for (Map.Entry<RegionInfo, ServerName> entry : hRegionInfoList) {
            serverCountMap.merge(entry.getValue(), 1, Integer::sum);
        }
        Assert.assertEquals(Math.min(getServerNameList().size(), regionLocations.size()), serverCountMap.size());

        int regionCount = 0;
        for (ServerName serverName : getServerNameList()) {
            List<RegionInfo> regionInfoList = getRegionInfoList(serverName, tableName);
            Assert.assertNotEquals(4, regionInfoList.size());
            regionCount += regionInfoList.size();
        }
        Assert.assertEquals(4, regionCount);
    }

    @Test
    public void testBalanceST2() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());

        TableName tableName2 = createAdditionalTable(tableName + "2");
        splitTable(tableName2, "a".getBytes());
        splitTable(tableName2, "b".getBytes());
        splitTable(tableName2, "c".getBytes());

        NavigableMap<RegionInfo, ServerName> regionLocations;
        List<Map.Entry<RegionInfo, ServerName>> hRegionInfoList;

        // for table1
        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertEquals(4, regionLocations.size());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(2).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(3).getValue());

        // for table2
        regionLocations = Util.getRegionLocationsMap(connection, tableName2);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertEquals(4, regionLocations.size());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(2).getValue());
        Assert.assertEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(3).getValue());

        // balance with st2
        String[] argsParam = {"zookeeper", ".*", "St2", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);
        command.run();

        validateST2(tableName);
        validateST2(tableName2);
    }

    private void validateST2(TableName table1) throws IOException {
        NavigableMap<RegionInfo, ServerName> regionLocations;
        List<Map.Entry<RegionInfo, ServerName>> hRegionInfoList;
        regionLocations = Util.getRegionLocationsMap(connection, table1);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Map<ServerName, Integer> serverCountMap = new HashMap<>();
        for (Map.Entry<RegionInfo, ServerName> entry : hRegionInfoList) {
            serverCountMap.merge(entry.getValue(), 1, Integer::sum);
        }
        Assert.assertEquals(Math.min(getServerNameList().size(), regionLocations.size()), serverCountMap.size());

        int regionCount;
        regionCount = 0;
        for (ServerName serverName : getServerNameList()) {
            List<RegionInfo> regionInfoList = getRegionInfoList(serverName, table1);
            Assert.assertNotEquals(4, regionInfoList.size());
            Assert.assertEquals(2, regionInfoList.size());
            regionCount += regionInfoList.size();
        }
        Assert.assertEquals(4, regionCount);
    }

    @Test
    public void testBalanceRR() throws Exception {
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

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "rR", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        command.run();

        regionLocations = Util.getRegionLocationsMap(connection, tableName);
        hRegionInfoList = new ArrayList<>(regionLocations.entrySet());
        Assert.assertNotEquals(hRegionInfoList.get(0).getValue(), hRegionInfoList.get(1).getValue());
        Assert.assertNotEquals(hRegionInfoList.get(2).getValue(), hRegionInfoList.get(3).getValue());
    }

    @Test
    public void testRule() throws Exception {
        Balance.reset();
        BalanceRule rule;
        List<RegionPlan> regionPlanListRR, regionPlanListRD;
        Set<TableName> tableNameSet = new HashSet<>();
        tableNameSet.add(tableName);

        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());

        // rule RR
        rule = BalanceRule.RR;
        regionPlanListRR = rule.makePlan(admin, tableNameSet, null);
        Assert.assertNotEquals(0, regionPlanListRR.size());
        for (RegionPlan regionPlan : regionPlanListRR) {
            // fixme
            Assert.assertNotEquals(regionPlan.getSource().getServerName(), regionPlan.getDestination().getServerName());
        }

        // rule RD
        int max_iteration = 1000;
        rule = BalanceRule.RD;
        for (int i = 0; i < max_iteration; i++) {
            regionPlanListRD = rule.makePlan(admin, tableNameSet, null);
            RegionPlan regionPlanRD = regionPlanListRD.get(0);
            RegionPlan regionPlanRR = regionPlanListRR.get(0);
            if (!regionPlanRD.getRegionInfo().equals(regionPlanRR.getRegionInfo())) {
                // RD plan should be different from RR plan
                break;
            }
            if (i == max_iteration - 1) {
                Assert.fail("RD rule is not correct");
            }
        }
    }

    @Test
    public void testBalanceRRDisabledTable() throws Exception {
        TableName tableName2 = createAdditionalTable(tableName + "2");
        admin.disableTable(tableName2);
        waitForDisabled(tableName2);

        String[] argsParam = {"zookeeper", tableName + ".*", "rr", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        // fixme
        command.run();
    }

    @Test
    public void testBalanceSTDisabledTable() throws Exception {
        TableName tableName2 = createAdditionalTable(tableName + "2");
        admin.disableTable(tableName2);
        waitForDisabled(tableName2);

        String[] argsParam = {"zookeeper", tableName + ".*", "st", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
        Balance command = new Balance(admin, args);

        command.run();
    }
}
