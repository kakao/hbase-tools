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

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.common.util.AlertSender;
import com.kakao.hbase.common.util.AlertSenderTest;
import com.kakao.hbase.stat.load.Level;
import com.kakao.hbase.stat.load.RegionName;
import com.kakao.hbase.stat.load.SortKey;
import com.kakao.hbase.stat.load.TableInfo;
import com.kakao.hbase.stat.print.Color;
import com.kakao.hbase.stat.print.Formatter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TableStatTest extends StatTestBase {
    public TableStatTest() {
        super(TableStatTest.class);
    }

    @Test
    public void testTableOneOfOne() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());

        List<HRegionInfo> regionInfoList = getRegionInfoList(tableName);

        String[] args = {"zookeeper", tableName, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        move(regionInfoList.get(0), serverNameList.get(1));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(regionInfoList.get(2), serverNameList.get(1));

        putData();
        waitForWriting(tableName, serverNameList.get(1), 3);
        command.run();

        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        assertNull(command.getLoad(new Level(serverNameList.get(0))));
        Assert.assertEquals(3L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));

        // iteration 2
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(0));
        move(regionInfoList.get(2), serverNameList.get(0));

        putData();
        waitForWriting(tableName, serverNameList.get(0), 3);
        command.run();

        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(3L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
        assertNull(command.getLoad(new Level(serverNameList.get(1))));
        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));

        // iteration 3
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(0));
        move(regionInfoList.get(2), serverNameList.get(1));

        updateWritingRequestMetric(tableName, serverNameList.get(0));
        updateWritingRequestMetric(tableName, serverNameList.get(1));
        putData();
        waitForWriting(tableName, serverNameList.get(0), 2);
        waitForWriting(tableName, serverNameList.get(1), 1);
        command.run();

        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(4L, command.getLoad(new Level(serverNameList.get(0))).get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad(new Level(serverNameList.get(1))).get(LoadEntry.Writes));
        Assert.assertEquals(5L, command.getLoad().getSummary().get(LoadEntry.Writes));
    }

    @Test
    public void testResetDiffStartPoint() throws Exception {
        String expected;

        splitTable("a".getBytes());
        splitTable("b".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());

        List<HRegionInfo> regionInfoList = getRegionInfoList(tableName);

        String[] args = {"zookeeper", tableName + ".*", "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        move(regionInfoList.get(0), serverNameList.get(1));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(regionInfoList.get(2), serverNameList.get(1));

        putData();
        waitForWriting(tableName, serverNameList.get(1), 3);
        command.run();

        expected = "Table                                             Reads    Writes   Regions  Files    FileSize  FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " UNIT_TEST_TableStatTest_testResetDiffStartPoint  0 | N/A  3 | N/A  3 | N/A  0 | N/A  0m | N/A  0m | N/A        0m | N/A      0 | N/A       \n" +
                " Total: 1                                         0 | N/A  3 | N/A  3 | N/A  0 | N/A  0m | N/A  0m | N/A        0m | N/A      0 | N/A       \n";
        Assert.assertEquals(expected, Color.clearColor(command.getFormatter().buildString(false, Formatter.Type.ANSI), Formatter.Type.ANSI));

        // resetDiffStartPoint
        command.getLoad().resetDiffStartPoint();
        Assert.assertEquals(0, command.getLoad().getTotalDuration());

        // iteration 2
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(0));
        move(regionInfoList.get(2), serverNameList.get(0));

        putData();
        waitForWriting(tableName, serverNameList.get(0), 3);
        command.run();

        expected = "Table                                             Reads    Writes   Regions  Files    FileSize  FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " UNIT_TEST_TableStatTest_testResetDiffStartPoint  0 | N/A  3 | N/A  3 | N/A  3 | N/A  0m | N/A  0m | N/A        0m | N/A      0 | N/A       \n" +
                " Total: 1                                         0 | N/A  3 | N/A  3 | N/A  3 | N/A  0m | N/A  0m | N/A        0m | N/A      0 | N/A       \n";
        assertEquals(expected, Color.clearColor(command.getFormatter().buildString(false, Formatter.Type.ANSI), Formatter.Type.ANSI));
    }

    private void putData() throws Exception {
        try (HTable table = (HTable) hConnection.getTable(tableName)) {
            Put put;
            put = new Put("0".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c".getBytes(), "0".getBytes());
            table.put(put);
            put = new Put("a".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c".getBytes(), "a".getBytes());
            table.put(put);
            put = new Put("b".getBytes());
            put.add(TEST_TABLE_CF.getBytes(), "c".getBytes(), "b".getBytes());
            table.put(put);
        }
    }

    @Test
    public void testTableOneOfTwo() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        String tableName2 = createAdditionalTable(tableName + "2");

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());

        List<HRegionInfo> regionInfoList = getRegionInfoList(tableName);
        regionInfoList.addAll(getRegionInfoList(tableName2));

        String[] args = {"zookeeper", tableName, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();

        move(regionInfoList.get(0), serverNameList.get(1));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(regionInfoList.get(2), serverNameList.get(1));
        move(regionInfoList.get(3), serverNameList.get(1));
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(0));
        move(regionInfoList.get(2), serverNameList.get(0));
        move(regionInfoList.get(3), serverNameList.get(0));
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        move(getRegionInfoList(tableName2).get(0), serverNameList.get(1));
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
    }

    @Test
    public void testRegion() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String[] args = {"zookeeper", tableName, "--region", "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<HRegionInfo> regionInfoList;

        // move regions to second RS for checking server index
        regionInfoList = getRegionInfoList(tableName);
        move(regionInfoList.get(0), serverNameList.get(1));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(regionInfoList.get(2), serverNameList.get(1));

        // iteration 1
        putData();
        waitForWriting(tableName, 3);
        command.run();

        Assert.assertEquals(3, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(1L, command.getLoad(new Level(new RegionName(regionInfoList.get(0), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad(new Level(new RegionName(regionInfoList.get(1), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad(new Level(new RegionName(regionInfoList.get(2), 0))).get(LoadEntry.Writes));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(2)));

        // check server index
        assertEquals(1, command.getTableInfo().serverIndex(regionInfoList.get(0)));

        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

        // iteration 2
        command.run();

        Assert.assertEquals(3, command.getLoad().getLoadMap().size());
        Assert.assertEquals(3, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(1L, command.getLoad(new Level(new RegionName(regionInfoList.get(0), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad(new Level(new RegionName(regionInfoList.get(1), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(1L, command.getLoad(new Level(new RegionName(regionInfoList.get(2), 0))).get(LoadEntry.Writes));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(2)));

        Assert.assertEquals(3L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));

        // iteration 3
        putData();
        waitForWriting(tableName, 6);
        command.toggleDiffFromStart();
        assertTrue(command.getTableInfo().getLoad().isDiffFromStart());
        assertEquals(0, command.getIntervalMS());
        command.run();

        Assert.assertEquals(3, command.getLoad().getLoadMap().size());
        Assert.assertEquals(3, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(2L, command.getLoad(new Level(new RegionName(regionInfoList.get(0), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(2L, command.getLoad(new Level(new RegionName(regionInfoList.get(1), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(2L, command.getLoad(new Level(new RegionName(regionInfoList.get(2), 0))).get(LoadEntry.Writes));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(2)));

        Assert.assertEquals(6L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));

        // iteration 4. diff_from_start is still true
        putData();
        waitForWriting(tableName, 9);
        assertTrue(command.getTableInfo().getLoad().isDiffFromStart());
        assertEquals(0, command.getIntervalMS());
        command.run();

        Assert.assertEquals(3, command.getLoad().getLoadMap().size());
        Assert.assertEquals(3, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(3L, command.getLoad(new Level(new RegionName(regionInfoList.get(0), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad(new Level(new RegionName(regionInfoList.get(1), 0))).get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad(new Level(new RegionName(regionInfoList.get(2), 0))).get(LoadEntry.Writes));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
        assertEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(2)));

        Assert.assertEquals(9L, command.getLoad().getSummary().get(LoadEntry.Writes));
        Assert.assertEquals(3L, command.getLoad().getSummaryPrev().get(LoadEntry.Writes));

        // iteration 5. move region
        move(regionInfoList.get(0), serverNameList.get(0));
        move(regionInfoList.get(1), serverNameList.get(1));
        command.run();

        Assert.assertEquals(3, command.getLoad().getLoadMap().size());
        Assert.assertEquals(3, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());

        regionInfoList = getRegionInfoList(tableName);
        Assert.assertEquals(1, command.getLoad(new Level(new RegionName(regionInfoList.get(0), 0))).get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad(new Level(new RegionName(regionInfoList.get(1), 0))).get(LoadEntry.Regions));
        Assert.assertEquals(1, command.getLoad(new Level(new RegionName(regionInfoList.get(2), 0))).get(LoadEntry.Regions));
        assertNotEquals(command.getTableInfo().serverIndex(regionInfoList.get(1)), command.getTableInfo().serverIndex(regionInfoList.get(0)));
    }

    @Test
    public void testRSWithTable() throws Exception {
        splitTable("a".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String[] args = {"zookeeper", tableName, "--rs", "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<HRegionInfo> regionInfoList;

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
            args = new String[]{"zookeeper", tableName, "--rs", "--interval=0"};
        }
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<HRegionInfo> regionInfoList;

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

            String tableName2 = createAdditionalTable(tableName + "2");

            List<ServerName> serverNameList = getServerNameList();
            assertEquals(RS_COUNT, serverNameList.size());
            String[] args = {"zookeeper", "--rs", "--interval=0"};
            TableStat command = new TableStat(admin, new StatArgs(args));

            ArrayList<HRegionInfo> regionInfoList;

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

    @Test
    public void testRSRegexWithTable() throws Exception {
        splitTable("a".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String rsRegex = serverNameList.get(0).getServerName() + ".*";
        String[] args = {"zookeeper", tableName, "--rs=" + rsRegex, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<HRegionInfo> regionInfoList;

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

        String tableName2 = createAdditionalTable(tableName + "2");

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String rsRegex = serverNameList.get(0).getServerName() + ".*";
        String[] args = {"zookeeper", "--rs=" + rsRegex, "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<HRegionInfo> regionInfoList;

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

        String tableName2 = createAdditionalTable(tableName + "2");

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String rsRegex = ".*";
        String[] args = {"zookeeper", "--rs=" + rsRegex, "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<HRegionInfo> regionInfoList;

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

    @Test
    public void testDisableEnableDrop() throws Exception {
        String[] args = {"zookeeper", tableName, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

        // iteration 2
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummaryPrev().size());

        // disable table
        admin.disableTable(tableName);
        waitForDisabled(tableName);

        // iteration 3
        command.run();
        Assert.assertEquals(0, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(0, command.getLoad().getSummary().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummaryPrev().size());

        // enable table
        admin.enableTable(tableName);
        waitForEnabled(tableName);

        // iteration 4
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

        // disable table
        admin.disableTable(tableName);
        waitForDisabled(tableName);

        // iteration 5
        command.run();
        Assert.assertEquals(0, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(0, command.getLoad().getSummary().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummaryPrev().size());

        // delete table
        admin.deleteTable(tableName);
        waitForDelete(tableName);

        // iteration 6
        command.run();
        Assert.assertEquals(0, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(0, command.getLoad().getSummary().size());
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());
    }

    @Test
    public void testAllTables() throws Exception {
        String expected = "Table                                   Reads    Writes   Regions  Files    FileSize  FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " UNIT_TEST_TableStatTest_testAllTables  0 | N/A  0 | N/A  0 | N/A  0 | N/A  0m | N/A  0m | N/A        0m | N/A      0 | N/A       \n" +
                " Total: 1                               0 | N/A  0 | N/A  0 | N/A  0 | N/A  0m | N/A  0m | N/A        0m | N/A      0 | N/A       \n";

        String[] args = {"zookeeper", "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());
        if (miniCluster) {
            assertEquals(expected, Color.clearColor(command.getFormatter().buildString(false,
                    Formatter.Type.ANSI), Formatter.Type.ANSI));
        }

        // disable table
        admin.disableTable(tableName);
        waitForDisabled(tableName);

        // iteration 2
        command.run();
        Assert.assertEquals(0, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(0, command.getLoad().getSummary().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummaryPrev().size());
    }

    @Test
    public void testSort() throws Exception {
        String[] args = {"zookeeper", "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        SortKey sortKeyPrev;

        sortKeyPrev = command.getLoad().getSortKey();
        assertEquals(SortKey.DEFAULT, sortKeyPrev);
        command.run();

        command.setSort(null);
        Assert.assertEquals(sortKeyPrev, command.getLoad().getSortKey());
        sortKeyPrev = command.getLoad().getSortKey();

        command.setSort("0");
        Assert.assertEquals(sortKeyPrev, command.getLoad().getSortKey());
        sortKeyPrev = command.getLoad().getSortKey();
        command.run();

        command.setSort("8");
        Assert.assertNotEquals(sortKeyPrev, command.getLoad().getSortKey());
        sortKeyPrev = command.getLoad().getSortKey();
        command.run();

        command.setSort("1");
        Assert.assertNotEquals(sortKeyPrev, command.getLoad().getSortKey());
        sortKeyPrev = command.getLoad().getSortKey();
        command.run();

        command.setSort("1d");
        Assert.assertEquals(sortKeyPrev, command.getLoad().getSortKey());
        sortKeyPrev = command.getLoad().getSortKey();
        command.run();

        command.setSort("@");
        Assert.assertNotEquals(sortKeyPrev, command.getLoad().getSortKey());
        command.run();
    }

    @Test
    public void testTables() throws Exception {
        String tableName2 = createAdditionalTable(tableName + "2");
        String tableName3 = createAdditionalTable(tableName + "22");

        String tableNameRegex;
        Set<String> tableSet;
        String[] tables;

        // test
        tableNameRegex = tableName + "2.*";
        tableSet = TableInfo.tables(admin, tableNameRegex);
        assertNotNull(tableSet);
        assertEquals(2, tableSet.size());
        tables = tableSet.toArray(new String[tableSet.size()]);
        assertEquals(tableName2, tables[0]);
        assertEquals(tableName3, tables[1]);

        // test
        tableNameRegex = tableName + ".*";
        tableSet = TableInfo.tables(admin, tableNameRegex);
        assertNotNull(tableSet);
        assertEquals(3, tableSet.size());
        tables = tableSet.toArray(new String[tableSet.size()]);
        assertEquals(tableName, tables[0]);
        assertEquals(tableName2, tables[1]);
        assertEquals(tableName3, tables[2]);

        // test
        tableNameRegex = tableName + "3.*";
        tableSet = TableInfo.tables(admin, tableNameRegex);
        assertNotNull(tableSet);
        assertEquals(0, tableSet.size());
    }

    @Test
    public void testRegexTableName() throws Exception {
        String tableName2 = createAdditionalTable(tableName + "2");
        String tableName3 = createAdditionalTable(tableName + "22");

        String tableNameRegex = tableName + "2.*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("Table", command.getLoad().getLevelClass().getLevelTypeString());
        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Set<Level> levelSet = command.getLoad().getLoadMap().keySet();
        Level[] levels = levelSet.toArray(new Level[command.getLoad().getLoadMap().size()]);
        assertEquals(tableName2, levels[0].toString());
        assertEquals(tableName3, levels[1].toString());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
    }

    @Test
    public void testRegexTableNameWithRS() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0", "--rs"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("RegionServer", command.getLoad().getLevelClass().getLevelTypeString());
    }

    @Test
    public void testRegexTableNameWithRegion() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0", "--region"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("Region (RS Index)", command.getLoad().getLevelClass().getLevelTypeString());
    }

    @Test
    public void testAfterFinished() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0", "--rs",
            "--" + Args.OPTION_AFTER_FINISHED + "=" + AlertSenderTest.ALERT_SCRIPT};
        TableStat command = new TableStat(admin, new StatArgs(args));

        int sendCountBefore = AlertSender.getSendCount();

        command.run();

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }

    @Test
    public void testAfterFailed() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=-1", "--rs",
            "--" + Args.OPTION_AFTER_FAILED + "=" + AlertSenderTest.ALERT_SCRIPT};
        TableStat command = new TableStat(admin, new StatArgs(args));

        int sendCountBefore = AlertSender.getSendCount();

        try {
            command.run();
            fail();
        } catch (IllegalArgumentException ignore) {
        }

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }
}