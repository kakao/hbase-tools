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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class SnapshotOptionAfterScriptTest extends TestBase {
    public SnapshotOptionAfterScriptTest() {
        super(SnapshotOptionAfterScriptTest.class);
    }

    @Test
    public void testAfterFailure() throws Exception {
        try {
            Snapshot.skipCheckTableExistence = true;

            class SnapshotArgsTest extends SnapshotArgs {
                public SnapshotArgsTest(String[] args) throws IOException {
                    super(args);
                }

                @Override
                public Set<TableName> tableSet(Admin admin) throws IOException {
                    Set<TableName> set = new TreeSet<>();
                    set.add(TableName.valueOf("INVALID_TABLE"));
                    return set;
                }
            }

            String[] argsParam;
            SnapshotArgs args;
            Snapshot app;

            argsParam = new String[]{"localhost", ".*",
                    "--" + Args.OPTION_AFTER_FAILURE + "=" + AlertSenderTest.ALERT_SCRIPT};
            args = new SnapshotArgsTest(argsParam);
            app = new Snapshot(connection, args);

            int sendCountBefore = AlertSender.getSendCount();
            try {
                app.run();
                Assert.fail();
            } catch (Throwable e) {
                if (e.getMessage() != null
                        && !e.getMessage().contains("Table 'INVALID_TABLE' doesn't exist, can't take snapshot"))
                    throw e;
            }
            Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
        } finally {
            Snapshot.skipCheckTableExistence = false;
        }
    }

    @Test
    public void testAfterSuccess() throws Exception {
        List<SnapshotDescription> snapshotDescriptions;

        // all tables, keep unlimited
        String[] argsParam = {"localhost", ".*", "--test",
            "--" + Args.OPTION_AFTER_SUCCESS + "=" + AlertSenderTest.ALERT_SCRIPT};
        SnapshotArgs args = new SnapshotArgs(argsParam);
        Snapshot app = new Snapshot(connection, args);

        int sendCountBefore = AlertSender.getSendCount();

        // create snapshot
        app.run();
        snapshotDescriptions = listSnapshots(tableName + ".*");
        assertEquals(1, snapshotDescriptions.size());

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }
}
