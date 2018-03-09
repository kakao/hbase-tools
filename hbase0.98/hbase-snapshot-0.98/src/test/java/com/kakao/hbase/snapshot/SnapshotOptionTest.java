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
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
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
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create tables
        createAdditionalTable(tableName + "2");
        createAdditionalTable(tableName + "3");

        // all tables, keep 2
        argsParam = new String[]{"localhost", ".*", "--keep=2", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot 1
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());
        for (HBaseProtos.SnapshotDescription description : snapshotDescriptions) {
            assertEquals(1, app.getMaxCount(description.getTable()));
        }

        // create snapshot 2
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());
        for (HBaseProtos.SnapshotDescription description : snapshotDescriptions) {
            assertEquals(2, app.getMaxCount(description.getTable()));
        }

        // create snapshot 3
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());
        for (HBaseProtos.SnapshotDescription description : snapshotDescriptions) {
            assertEquals(3, app.getMaxCount(description.getTable()));
        }

        // create snapshot 4
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());
        for (HBaseProtos.SnapshotDescription description : snapshotDescriptions) {
            assertEquals(3, app.getMaxCount(description.getTable()));
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
            app = new Snapshot(admin, args);
            app.run();
            fail();
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains("keep count should be a positive number"))
                throw e;
        }
    }

    @Test
    public void testSkipFlush() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // skip flush
        argsParam = new String[]{"localhost", ".*", "--skip-flush=true", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
        assertEquals(HBaseProtos.SnapshotDescription.Type.SKIPFLUSH, snapshotDescriptions.get(0).getType());

        // do not skip flush
        argsParam = new String[]{"localhost", ".*", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(HBaseProtos.SnapshotDescription.Type.FLUSH, snapshotDescriptions.get(1).getType());
    }

    @Test
    public void testExclude() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        String tableName2 = createAdditionalTable(tableName + "2");

        // with table list
        argsParam = new String[]{"localhost", ".*", "--exclude=" + tableName, "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
    }

    @Test
    public void testClearWatchLeak() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // with option
        argsParam = new String[]{"localhost", ".*", "--test", "--clear-watch-leak"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
    }

    @Test
    public void testExcludeRegexList() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        String tableName2 = createAdditionalTable(tableName + "2");
        createAdditionalTable(tableName + "21");

        // with table list
        argsParam = new String[]{"localhost", ".*", "--exclude=" + tableName2 + ".*", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
        assertEquals(tableName, snapshotDescriptions.get(0).getTable());
    }

    @Test
    public void testOverride() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        String tableName2 = createAdditionalTable(tableName + "2");

        // with table list
        argsParam = new String[]{"localhost", ".*", "--override=" + tableName + "/1/true", "--keep=2", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot 1
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
        assertEquals(HBaseProtos.SnapshotDescription.Type.FLUSH, snapshotDescriptions.get(0).getType());
        assertEquals(tableName, snapshotDescriptions.get(1).getTable());
        assertEquals(HBaseProtos.SnapshotDescription.Type.SKIPFLUSH, snapshotDescriptions.get(1).getType());

        // create snapshot 2
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
        assertEquals(HBaseProtos.SnapshotDescription.Type.FLUSH, snapshotDescriptions.get(0).getType());
        assertEquals(tableName2, snapshotDescriptions.get(1).getTable());
        assertEquals(HBaseProtos.SnapshotDescription.Type.FLUSH, snapshotDescriptions.get(1).getType());
        assertEquals(tableName, snapshotDescriptions.get(2).getTable());
        assertEquals(HBaseProtos.SnapshotDescription.Type.SKIPFLUSH, snapshotDescriptions.get(2).getType());
    }

    @Test
    public void testDeleteSnapshotsForNotExistingTables() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        String tableName2 = createAdditionalTable(tableName + "2");

        // create snapshot for 2 tables and keep 1
        argsParam = new String[]{"localhost", ".*", "--keep=1", "--test", "--delete-snapshot-for-not-existing-table"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot first
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
        assertEquals(tableName, snapshotDescriptions.get(1).getTable());

        // create snapshot once more
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
        assertEquals(tableName, snapshotDescriptions.get(1).getTable());

        // create snapshot does not follow hbase-snapshot naming rule
        String shouldNotBeDeleted = tableName2 + "-snapshot-test";
        admin.snapshot(shouldNotBeDeleted, tableName2);

        // drop one table
        dropTable(tableName2);

        // create snapshot for only one table and snapshot for dropped table should not be deleted yet
        argsParam = new String[]{"localhost", ".*", "--keep=1", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
        assertEquals(tableName2, snapshotDescriptions.get(1).getTable());
        assertEquals(tableName, snapshotDescriptions.get(2).getTable());

        // create snapshot for only one table and delete snapshot for dropped table
        argsParam = new String[]{"localhost", ".*", "--keep=1", "--test", "--delete-snapshot-for-not-existing-table"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());
        assertEquals(tableName2, snapshotDescriptions.get(0).getTable());
        assertEquals(shouldNotBeDeleted, snapshotDescriptions.get(0).getName());
        assertEquals(tableName, snapshotDescriptions.get(1).getTable());
    }
}
