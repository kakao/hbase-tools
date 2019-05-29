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

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TableStatTableArgTest extends StatTestBase {
    public TableStatTableArgTest() {
        super(TableStatTableArgTest.class);
    }

    @Test
    public void testTableOneOfOne() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());

        List<RegionInfo> regionInfoList = getRegionInfoList(tableName);

        String[] args = {"zookeeper", tableName.getNameAsString(), "--interval=0"};
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
    public void testTableOneOfTwo() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        TableName tableName2 = createAdditionalTable(tableName + "2");

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());

        List<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        regionInfoList.addAll(getRegionInfoList(tableName2));

        String[] args = {"zookeeper", tableName.getNameAsString(), "--interval=0"};
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
    public void testTableName() throws Exception {
        createAdditionalTable(TableName.valueOf(tableName + "2"));
        createAdditionalTable(TableName.valueOf(tableName + "22"));

        String[] args = {"zookeeper", tableName.getNameAsString(), "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("RegionServer", command.getLoad().getLevelClass().getLevelTypeString());
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
    }
}
