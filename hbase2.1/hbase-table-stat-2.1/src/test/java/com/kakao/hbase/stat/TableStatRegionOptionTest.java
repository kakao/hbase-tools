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
import com.kakao.hbase.stat.load.RegionName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TableStatRegionOptionTest extends StatTestBase {
    public TableStatRegionOptionTest() {
        super(TableStatRegionOptionTest.class);
    }

    @Test
    public void testRegion() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());
        String[] args = {"zookeeper", tableName.getNameAsString(), "--region", "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        ArrayList<RegionInfo> regionInfoList;

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
}
