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
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.util.AlertSender;
import com.kakao.hbase.common.util.AlertSenderTest;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SnapshotTest extends TestBase {
    public SnapshotTest() {
        super(SnapshotTest.class);
    }

    @Test
    public void testAllTables() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;

        // create tables
        createAdditionalTable(tableName + "2");
        createAdditionalTable(tableName + "3");

        // all tables, keep unlimited
        String[] argsParam = {"localhost", ".*", "--test"};
        SnapshotArgs args = new SnapshotArgs(argsParam);
        Snapshot app = new Snapshot(admin, args);

        // create snapshot 1
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());

        // create snapshot 2
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());

        // create snapshot 3
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(9, snapshotDescriptions.size());

        // create snapshot 3
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(12, snapshotDescriptions.size());
    }

    @Test
    public void testRegexList() throws Exception {
        // create tables
        String tableName2 = createAdditionalTable(tableName + "2");
        String tableName3 = createAdditionalTable(tableName + "3");

        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;

        // all tables, keep unlimited
        String[] argsParam = {"localhost", tableName + ".*," + tableName2 + ".*," + tableName3 + ".*"};
        SnapshotArgs args = new SnapshotArgs(argsParam);
        Snapshot app = new Snapshot(admin, args);

        // create snapshot 1
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(3, snapshotDescriptions.size());

        // create snapshot 2
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(6, snapshotDescriptions.size());

        // create snapshot 3
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(9, snapshotDescriptions.size());

        // create snapshot 3
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(12, snapshotDescriptions.size());
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
    public void testDuplicatedName() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        argsParam = new String[]{"localhost", ".*", "--test"};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        String snapshotName = app.getPrefix(tableName) + "test";
        // create snapshot first
        app.snapshot(null, tableName, snapshotName);
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());

        // create snapshot again
        app.snapshot(null, tableName, snapshotName);
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());
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
    public void testNamespace() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;
        String fullTableName = null;

        try {
            // create table with namespace
            fullTableName = TEST_NAMESPACE + ":" + TestBase.tableName;
            createTable(fullTableName);

            // table with namespace
            argsParam = new String[]{"localhost", fullTableName};
            args = new SnapshotArgs(argsParam);
            app = new Snapshot(admin, args);

            // create snapshot
            app.run();
            snapshotDescriptions = admin.listSnapshots(app.getPrefix(fullTableName) + ".*");
            assertEquals(1, snapshotDescriptions.size());
        } finally {
            dropTable(fullTableName);
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
    public void testList() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        String tableName2 = createAdditionalTable(tableName + "2");

        // with table list
        argsParam = new String[]{"localhost", tableName + "," + tableName2};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(2, snapshotDescriptions.size());

        // with table list contains blank
        argsParam = new String[]{"localhost", tableName + " , " + tableName2};
        args = new SnapshotArgs(argsParam);
        app = new Snapshot(admin, args);

        // create snapshot
        Thread.sleep(1000);
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(4, snapshotDescriptions.size());
    }

    @Test
    public void testDetailList() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;
        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        // create table
        String tableName2 = createAdditionalTable(tableName + "2");

        // with table list
        argsParam = new String[]{"localhost", tableName + "/1/true," + tableName2 + "/2/false"};
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
    public void testAfterFailure() throws Exception {
        class SnapshotArgsTest extends SnapshotArgs {
            public SnapshotArgsTest(String[] args) throws IOException {
                super(args);
            }

            @Override
            public Set<String> tableSet(HBaseAdmin admin) throws IOException {
                Set<String> set = new TreeSet<>();
                set.add("INVALID_TABLE");
                return set;
            }
        }

        String[] argsParam;
        SnapshotArgs args;
        Snapshot app;

        argsParam = new String[]{"localhost", ".*",
            "--" + Args.OPTION_AFTER_FAILURE + "=" + AlertSenderTest.ALERT_SCRIPT};
        args = new SnapshotArgsTest(argsParam);
        app = new Snapshot(admin, args);

        int sendCountBefore = AlertSender.getSendCount();
        try {
            app.run();
            Assert.fail();
        } catch (Throwable e) {
            if (!e.getMessage().contains("Table 'INVALID_TABLE' doesn't exist, can't take snapshot"))
                throw e;
        }
        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }

    @Test
    public void testAfterSuccess() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotDescriptions;

        // all tables, keep unlimited
        String[] argsParam = {"localhost", ".*", "--test",
            "--" + Args.OPTION_AFTER_SUCCESS + "=" + AlertSenderTest.ALERT_SCRIPT};
        SnapshotArgs args = new SnapshotArgs(argsParam);
        Snapshot app = new Snapshot(admin, args);

        int sendCountBefore = AlertSender.getSendCount();

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }
}