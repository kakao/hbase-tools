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

package com.kakao.hbase.snapshot;

import com.kakao.hbase.SnapshotArgs;
import com.kakao.hbase.TestBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SnapshotOptionTest extends TestBase {
    public SnapshotOptionTest() {
        super(SnapshotOptionTest.class);
    }

    @Test
    public void testAllTablesWithKeep() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create tables
        createAdditionalTable(TableName.valueOf(tableName + "2"));
        createAdditionalTable(TableName.valueOf(tableName + "3"));

        // all tables, keep 2
        argsParam = new String[]{"localhost", ".*", "--keep=2", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot 1
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());
        for (SnapshotDescription description : snapshotDescriptions) {
            assertEquals(1, app.getMaxCount(description.getTableName()));
        }

        // create snapshot 2
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());
        for (SnapshotDescription description : snapshotDescriptions) {
            assertEquals(2, app.getMaxCount(description.getTableName()));
        }

        // create snapshot 3
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());
        for (SnapshotDescription description : snapshotDescriptions) {
            assertEquals(3, app.getMaxCount(description.getTableName()));
        }

        // create snapshot 4
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());
        for (SnapshotDescription description : snapshotDescriptions) {
            assertEquals(3, app.getMaxCount(description.getTableName()));
        }
    }

    @Test
    public void testInvalidKeep() throws Exception {
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        argsParam = new String[]{"localhost", ".*", "--keep=-2"};
        try {
            args = new SnapshotArgs(argsParam);
            app = new Snapshot(connection, args);
            app.run();
            fail();
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains("keep count should be a positive number"))
                throw e;
        }
    }

    @Test
    public void testSkipFlush() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // skip flush
        argsParam = new String[]{"localhost", ".*", "--skip-flush=true", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
        assertEquals(SnapshotType.SKIPFLUSH, snapshotDescriptions.get(0).getType());

        // do not skip flush
        argsParam = new String[]{"localhost", ".*", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(SnapshotType.FLUSH, snapshotDescriptions.get(1).getType());
    }

    @Test
    public void testExclude() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        TableName tableName2 = createAdditionalTable(tableName + "2");

        // with table list
        argsParam = new String[]{"localhost", ".*", "--exclude=" + tableName, "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
    }

    @Test
    public void testClearWatchLeak() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // with option
        argsParam = new String[]{"localhost", ".*", "--test", "--clear-watch-leak"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
    }

    @Test
    public void testExcludeRegexList() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        TableName tableName2 = createAdditionalTable(tableName + "2");
        createAdditionalTable(TableName.valueOf(tableName + "21"));

        // with table list
        argsParam = new String[]{"localhost", ".*", "--exclude=" + tableName2 + ".*", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
        assertEquals(tableName, snapshotDescriptions.get(0).getTableName());
    }

    @Test
    public void testOverride() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        TableName tableName2 = createAdditionalTable(tableName + "2");

        // with table list
        argsParam = new String[]{"localhost", ".*", "--override=" + tableName + "/1/true", "--keep=2", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot 1
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
        assertEquals(SnapshotType.FLUSH, snapshotDescriptions.get(0).getType());
        assertEquals(tableName, snapshotDescriptions.get(1).getTableName());
        assertEquals(SnapshotType.SKIPFLUSH, snapshotDescriptions.get(1).getType());

        // create snapshot 2
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
        assertEquals(SnapshotType.FLUSH, snapshotDescriptions.get(0).getType());
        assertEquals(tableName2, snapshotDescriptions.get(1).getTableName());
        assertEquals(SnapshotType.FLUSH, snapshotDescriptions.get(1).getType());
        assertEquals(tableName, snapshotDescriptions.get(2).getTableName());
        assertEquals(SnapshotType.SKIPFLUSH, snapshotDescriptions.get(2).getType());
    }

    @Test
    public void testDeleteSnapshotsForNotExistingTables() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        TableName tableName2 = createAdditionalTable(tableName + "2");

        // create snapshot for 2 tables and keep 1
        argsParam = new String[]{"localhost", ".*", "--keep=1", "--test", "--delete-snapshot-for-not-existing-table"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);

        // create snapshot first
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
        assertEquals(tableName, snapshotDescriptions.get(1).getTableName());

        // create snapshot once more
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
        assertEquals(tableName, snapshotDescriptions.get(1).getTableName());

        // create snapshot does not follow hbase-snapshot naming rule
        String shouldNotBeDeleted = tableName2 + "-snapshot-test";
        try (Admin admin = connection.getAdmin()) {
            admin.snapshot(shouldNotBeDeleted, tableName2);
        }

        // drop one table
        dropTable(tableName2);

        // create snapshot for only one table and snapshot for dropped table should not be deleted yet
        argsParam = new String[]{"localhost", ".*", "--keep=1", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
        assertEquals(tableName2, snapshotDescriptions.get(1).getTableName());
        assertEquals(tableName, snapshotDescriptions.get(2).getTableName());

        // create snapshot for only one table and delete snapshot for dropped table
        argsParam = new String[]{"localhost", ".*", "--keep=1", "--test", "--delete-snapshot-for-not-existing-table"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(connection, args);
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTableName());
        assertEquals(shouldNotBeDeleted, snapshotDescriptions.get(0).getName());
        assertEquals(tableName, snapshotDescriptions.get(1).getTableName());
    }
}
