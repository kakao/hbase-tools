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
import com.kakao.hbase.common.util.AlertSender;
import com.kakao.hbase.common.util.AlertSenderTest;
import com.kakao.hbase.stat.load.Level;
import com.kakao.hbase.stat.load.SortKey;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TableStatOptionTest extends StatTestBase {
    public TableStatOptionTest() {
        super(TableStatOptionTest.class);
    }

    @Test
    public void testResetDiffStartPoint() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        List<ServerName> serverNameList = getServerNameList();
        assertEquals(RS_COUNT, serverNameList.size());

        List<RegionInfo> regionInfoList = getRegionInfoList(tableName);

        String[] args = {"zookeeper", tableName + ".*", "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        move(regionInfoList.get(0), serverNameList.get(1));
        move(regionInfoList.get(1), serverNameList.get(1));
        move(regionInfoList.get(2), serverNameList.get(1));

        putData();
        waitForWriting(tableName, serverNameList.get(1), 3);
        command.run();

        assertNull(command.getLoad().getLoadMapPrev().get(new Level(tableName)));

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

        assertNull(command.getLoad().getLoadMapPrev().get(new Level(tableName)));
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
    public void testAfterSuccess() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0", "--rs",
            "--" + Args.OPTION_AFTER_SUCCESS + "=" + AlertSenderTest.ALERT_SCRIPT};
        TableStat command = new TableStat(admin, new StatArgs(args));

        int sendCountBefore = AlertSender.getSendCount();

        command.run();

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }

    @Test
    public void testAfterFailure() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=-1", "--rs",
            "--" + Args.OPTION_AFTER_FAILURE + "=" + AlertSenderTest.ALERT_SCRIPT};
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
