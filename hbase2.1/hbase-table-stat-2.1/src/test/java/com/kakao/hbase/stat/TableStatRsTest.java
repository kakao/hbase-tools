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
import com.kakao.hbase.stat.load.Level;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableStatRsTest extends StatTestBase {
    public TableStatRsTest() {
        super(TableStatRsTest.class);
    }

    @Test
    public void testRSWithTable() throws Exception {
        splitTable("a".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String[] args = {"zookeeper", tableName.getNameAsString(), "--rs", "--interval=0"};
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

        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(1L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
        Assert.assertEquals(2L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Regions));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(0)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(1)));

        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

        // iteration 2
        putData();
        waitForWriting(tableName, 6);
        command.run();

        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(2, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(2L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
        Assert.assertEquals(4L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Regions));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(0)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(1)));

        Assert.assertEquals(6L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));
    }

    @Test
    public void testRSWith1Table() throws Exception {
        splitTable("a".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String[] args;
        if (miniCluster) {
            args = new String[]{"zookeeper", "--rs", "--interval=0"};
        } else {
            args = new String[]{"zookeeper", tableName.getNameAsString(), "--rs", "--interval=0"};
        }
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

        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(1L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
        Assert.assertEquals(2L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Regions));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(0)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(1)));

        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

        // iteration 2
        putData();
        waitForWriting(tableName, 6);
        command.run();

        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(2, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(2L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
        Assert.assertEquals(4L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Regions));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(0)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(1)));

        Assert.assertEquals(6L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));
    }

    @Test
    public void testRSWith2Tables() throws Exception {
        if (miniCluster) {
            splitTable("a".getBytes());

            TableName tableName2 = createAdditionalTable(tableName + "2");

            List<ServerName> serverNameList = getServerNameList();
            assertEquals(RS_COUNT, serverNameList.size());
            String[] args = {"zookeeper", "--rs", "--interval=0"};
            TableStat command = new TableStat(admin, new StatArgs(args));

            ArrayList<RegionInfo> regionInfoList;

            // move regions to second RS for checking server index
            regionInfoList = getRegionInfoList(tableName);
            move(regionInfoList.get(0), serverNameList.get(0));
            move(regionInfoList.get(1), serverNameList.get(1));
            move(getRegionInfoList(tableName2).get(0), serverNameList.get(1));

            // iteration 1
            putData();
            waitForWriting(tableName, 3);
            command.run();

            Assert.assertEquals(2, command.getLoad().getLoadMap().size());
            Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
            Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

            regionInfoList = getRegionInfoList(tableName);
            Assert.assertEquals(1L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
            Assert.assertEquals(2L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
            Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Regions));
            Assert.assertEquals(2, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Regions));
            assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(0)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
            assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(1)));

            Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));
            Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

            // iteration 2
            putData();
            waitForWriting(tableName, 6);
            command.run();

            Assert.assertEquals(2, command.getLoad().getLoadMap().size());
            Assert.assertEquals(2, command.getLoad().getLoadMapPrev().size());
            Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

            regionInfoList = getRegionInfoList(tableName);
            Assert.assertEquals(2L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
            Assert.assertEquals(4L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
            Assert.assertEquals(1, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Regions));
            Assert.assertEquals(2, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Regions));
            assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(0)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
            assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(1)));

            Assert.assertEquals(6L, command.getLoad().getSummary().get(LoadEntry.Writes));
            Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));
        }
    }
}
