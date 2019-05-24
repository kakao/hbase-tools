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

public class SnapshotTableArgTest extends TestBase {
    public SnapshotTableArgTest() {
        super(SnapshotTableArgTest.class);
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
}