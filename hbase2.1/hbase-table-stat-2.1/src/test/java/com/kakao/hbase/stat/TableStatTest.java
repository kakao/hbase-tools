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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableStatTest extends StatTestBase {
    public TableStatTest() {
        super(TableStatTest.class);
    }

    @Test
    public void testDisableEnableDrop() throws Exception {
        String[] args = {"zookeeper", tableName.getNameAsString(), "--interval=0"};
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
        Admin admin = connection.getAdmin();
        System.out.println(admin.listTableDescriptors(Pattern.compile(".*")));
        admin.disableTable(tableName);
        waitForDisabled(tableName);
        System.out.println(admin.isTableDisabled(tableName));
        System.out.println(admin.listTableDescriptors(Pattern.compile(".*")));

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
        try (Admin admin = connection.getAdmin()) {
            admin.disableTable(tableName);
        }
        waitForDisabled(tableName);

        // iteration 2
        command.run();
        Assert.assertEquals(0, command.getLoad().getLoadMap().size());
        Assert.assertEquals(1, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(0, command.getLoad().getSummary().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummaryPrev().size());
    }

    @Test
    public void testTableRegex() throws Exception {
        createAdditionalTable(TableName.valueOf(tableName + "2"));

        String[] args = {"zookeeper", ".*", "--interval=0", "--test"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        // iteration 1
        command.run();
        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(0, command.getLoad().getSummaryPrev().size());

        // disable table
        try (Admin admin = connection.getAdmin()) {
            admin.disableTable(tableName);
        }
        waitForDisabled(tableName);

        // iteration 2
        command.run();
        Assert.assertEquals(1, command.getLoad().getLoadMap().size());
        Assert.assertEquals(2, command.getLoad().getLoadMapPrev().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummary().size());
        Assert.assertEquals(loadEntryLength, command.getLoad().getSummaryPrev().size());
    }

    @Test
    public void testTables() throws Exception {
        TableName tableName2 = createAdditionalTable(tableName + "2");
        TableName tableName3 = createAdditionalTable(tableName + "22");

        String tableNameRegex;
        Set<TableName> tableSet;
        TableName[] tables;
        String[] argsParam;
        StatArgs args;

        // test
        tableNameRegex = tableName + "2.*";
        argsParam = new String[]{"zookeeper", tableNameRegex, "--interval=0", "--test"};
        args = new StatArgs(argsParam);
        tableSet = Args.tables(args, admin);
        assertNotNull(tableSet);
        assertEquals(2, tableSet.size());
        tables = tableSet.toArray(new TableName[0]);
        assertEquals(tableName2, tables[0]);
        assertEquals(tableName3, tables[1]);

        // test
        tableNameRegex = tableName + ".*";
        argsParam = new String[]{"zookeeper", tableNameRegex, "--interval=0", "--test"};
        args = new StatArgs(argsParam);
        tableSet = Args.tables(args, admin);
        assertNotNull(tableSet);
        assertEquals(3, tableSet.size());
        tables = tableSet.toArray(new TableName[0]);
        assertEquals(tableName, tables[0]);
        assertEquals(tableName2, tables[1]);
        assertEquals(tableName3, tables[2]);

        // test
        tableNameRegex = tableName + "3.*";
        argsParam = new String[]{"zookeeper", tableNameRegex, "--interval=0", "--test"};
        args = new StatArgs(argsParam);
        tableSet = Args.tables(args, admin);
        assertNotNull(tableSet);
        assertEquals(0, tableSet.size());
    }
}
