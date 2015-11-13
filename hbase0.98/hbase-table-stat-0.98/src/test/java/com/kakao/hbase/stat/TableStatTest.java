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
import com.kakao.hbase.stat.print.Color;
import com.kakao.hbase.stat.print.Formatter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableStatTest extends StatTestBase {
    public TableStatTest() {
        super(TableStatTest.class);
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
        String[] args = {"zookeeper", "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

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
    public void testTables() throws Exception {
        String tableName2 = createAdditionalTable(tableName + "2");
        String tableName3 = createAdditionalTable(tableName + "22");

        String tableNameRegex;
        Set<String> tableSet;
        String[] tables;

        // test
        tableNameRegex = tableName + "2.*";
        tableSet = Args.tables(admin, tableNameRegex);
        assertNotNull(tableSet);
        assertEquals(2, tableSet.size());
        tables = tableSet.toArray(new String[tableSet.size()]);
        assertEquals(tableName2, tables[0]);
        assertEquals(tableName3, tables[1]);

        // test
        tableNameRegex = tableName + ".*";
        tableSet = Args.tables(admin, tableNameRegex);
        assertNotNull(tableSet);
        assertEquals(3, tableSet.size());
        tables = tableSet.toArray(new String[tableSet.size()]);
        assertEquals(tableName, tables[0]);
        assertEquals(tableName2, tables[1]);
        assertEquals(tableName3, tables[2]);

        // test
        tableNameRegex = tableName + "3.*";
        tableSet = Args.tables(admin, tableNameRegex);
        assertNotNull(tableSet);
        assertEquals(0, tableSet.size());
    }
}