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

package com.kakao.hbase.stat;

import com.kakao.hbase.common.LoadEntry;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TableStatRsRegexTest extends StatTestBase {
    public TableStatRsRegexTest() {
        super(TableStatRsRegexTest.class);
    }

    @Test
    public void testRSRegexWithTable() throws Exception {
        splitTable("a".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String rsRegex = serverNameList.get(0).getServerName() + ".*";
        String[] args = {"zookeeper", tableName.getNameAsString(), "--rs=" + rsRegex, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<RegionInfo> regionInfoList;

        // move regions to second RS for checking server index
        regionInfoList = getRegionInfoList(tableName);
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(1));

        // iteration 1
        putData();
        waitForWriting(tableName, 3);
        command.run();

        assertEquals(ServerName.class, command.getLoad().getLevelClass().getLevelClass());
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        Assert.assertEquals(1L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());
        Assert.assertEquals(1, command.getLoad().getSummary().get(LoadEntry.Regions));
        assertNull(command.getLoad().getSummaryPrev().get(LoadEntry.Regions));

        // iteration 2
        putData();
        waitForWriting(tableName, 6);
        command.run();

        assertEquals(ServerName.class, command.getLoad().getLevelClass().getLevelClass());
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        Assert.assertEquals(2L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));
        Assert.assertEquals(1, command.getLoad().getSummary().get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad().getSummaryPrev().get(LoadEntry.Regions));
    }

    @Test
    public void testRSRegexWithAllTable() throws Exception {
        if (!miniCluster) return;

        splitTable("a".getBytes());

        TableName tableName2 = createAdditionalTable(tableName + "2");

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String rsRegex = serverNameList.get(0).getServerName() + ".*";
        String[] args = {"zookeeper", "--rs=" + rsRegex, "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<RegionInfo> regionInfoList;

        // move regions to second RS for checking server index
        regionInfoList = getRegionInfoList(tableName);
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(getRegionInfoList(tableName2).get(0), serverNameList.get(0));

        // iteration 1
        putData();
        waitForWriting(tableName, 3);
        command.run();

        assertEquals(ServerName.class, command.getLoad().getLevelClass().getLevelClass());
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        Assert.assertEquals(1L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());
        Assert.assertEquals(2, command.getLoad().getSummary().get(LoadEntry.Regions));
        assertNull(command.getLoad().getSummaryPrev().get(LoadEntry.Regions));

        // iteration 2
        putData();
        waitForWriting(tableName, 6);
        command.run();

        assertEquals(ServerName.class, command.getLoad().getLevelClass().getLevelClass());
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        Assert.assertEquals(2L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));
        Assert.assertEquals(2, command.getLoad().getSummary().get(LoadEntry.Regions));
        Assert.assertEquals(2, command.getLoad().getSummaryPrev().get(LoadEntry.Regions));
    }

    @Test
    public void testRSRegexMultiRS() throws Exception {
        if (!miniCluster) return;

        splitTable("a".getBytes());

        TableName tableName2 = createAdditionalTable(tableName + "2");

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String rsRegex = ".*";
        String[] args = {"zookeeper", "--rs=" + rsRegex, "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<RegionInfo> regionInfoList;

        // move regions to second RS for checking server index
        regionInfoList = getRegionInfoList(tableName);
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(getRegionInfoList(tableName2).get(0), serverNameList.get(0));

        // iteration 1
        putData();
        waitForWriting(tableName, 3);
        command.run();

        assertEquals(ServerName.class, command.getLoad().getLevelClass().getLevelClass());
        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());
        Assert.assertEquals(3, command.getLoad().getSummary().get(LoadEntry.Regions));
        assertNull(command.getLoad().getSummaryPrev().get(LoadEntry.Regions));

        // iteration 2
        putData();
        waitForWriting(tableName, 6);
        command.run();

        assertEquals(ServerName.class, command.getLoad().getLevelClass().getLevelClass());
        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(2, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        Assert.assertEquals(6L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));
        Assert.assertEquals(3, command.getLoad().getSummary().get(LoadEntry.Regions));
        Assert.assertEquals(3, command.getLoad().getSummaryPrev().get(LoadEntry.Regions));
    }
}
