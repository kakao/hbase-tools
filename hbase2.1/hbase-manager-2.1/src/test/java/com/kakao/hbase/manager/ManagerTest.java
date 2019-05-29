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

package com.kakao.hbase.manager;

import com.kakao.hbase.TestBase;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.InvalidTableException;
import com.kakao.hbase.common.util.AlertSender;
import com.kakao.hbase.common.util.AlertSenderTest;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.manager.command.Command;
import joptsimple.OptionException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.Assert;
import org.junit.Test;

public class ManagerTest extends TestBase {
    public ManagerTest() {
        super(ManagerTest.class);
    }

    @Test
    public void testDisabledTable() throws Exception {
        admin.disableTable(tableName);
        try {
            Util.validateTable(admin, tableName.getNameAsString());
        } catch (InvalidTableException e) {
            if (!e.getMessage().contains("Table is not enabled")) {
                throw e;
            }
        }
    }

    @Test
    public void testExistingTable() throws Exception {
        Util.validateTable(admin, tableName.getNameAsString());
    }

    @Test
    public void testNotExistingTable() throws Exception {
        try {
            Util.validateTable(admin, tableName + "2");
        } catch (InvalidTableException e) {
            if (!e.getMessage().contains("Table does not exist")) {
                throw e;
            }
        }
    }

    @Test
    public void testUsage() throws Exception {
        for (Class<? extends Command> commandClass : Manager.getCommandSet()) {
            String usage = Manager.getCommandUsage(commandClass.getSimpleName());
            Assert.assertNotNull(usage);
            Assert.assertTrue(usage.length() > 0);
        }
    }

    @Test
    public void testInvalidCommand() throws Exception {
        String[] args = {"command", "zookeeper", "table"};
        try {
            Manager.parseArgs(args);
            Assert.fail();
        } catch (RuntimeException e) {
            if (!e.getMessage().contains(Manager.INVALID_COMMAND)) throw e;
        }
    }

    @Test
    public void testInvalidOption() throws Exception {
        String[] args = {"balance", "zookeeper", "table", "--nonono"};
        try {
            Manager.parseArgs(args);
            Assert.fail();
        } catch (OptionException ignored) {
        }
    }

    @Test
    public void testRegexTable() throws Exception {
        Util.validateTable(admin, tableName + ".*");
    }

    @Test
    public void testTableList() throws Exception {
        TableName tableName2 = TableName.valueOf(tableName + "2");
        try {
            createTable(tableName2);
            Util.validateTable(admin, tableName + "," + tableName2);
        } finally {
            dropTable(tableName2);
        }
    }

    @Test
    public void testNoArg() throws Exception {
        String[] args = {};
        try {
            Manager.parseArgs(args);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testNamespace() throws Exception {
        TableName tn = TableName.valueOf(TEST_NAMESPACE, tableName.getNameAsString());

        HTableDescriptor td = new HTableDescriptor(tn);
        HColumnDescriptor cd = new HColumnDescriptor(TEST_TABLE_CF);
        td.addFamily(cd);

        admin.createTable(td);

        Util.validateTable(admin, TEST_NAMESPACE + ":" + tableName);

        admin.disableTable(tn);
        admin.deleteTable(tn);
    }

    @Test
    public void testManager() throws Exception {
        String commandName = "assign";
        String[] args = {commandName, "zookeeper", "table"};
        Args argsObject = Manager.parseArgs(args);
        new Manager(connection, argsObject, commandName);
    }

    @Test
    public void testAfterFailure() throws Exception {
        String commandName = "assign";
        String[] args = {commandName, "localhost", "balancer", "invalid",
                "--" + Args.OPTION_AFTER_FAILURE + "=" + AlertSenderTest.ALERT_SCRIPT};
        Args argsObject = Manager.parseArgs(args);
        Manager manager = new Manager(connection, argsObject, commandName);

        int sendCountBefore = AlertSender.getSendCount();

        try {
            manager.run();
            Assert.fail();
        } catch (IllegalArgumentException ignore) {
        }

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }

    @Test
    public void testAfterSuccess() throws Exception {
        String commandName = "assign";
        String[] args = {commandName, "localhost", "balancer", "on",
                "--" + Args.OPTION_AFTER_SUCCESS + "=" + AlertSenderTest.ALERT_SCRIPT};
        Args argsObject = Manager.parseArgs(args);
        Manager manager = new Manager(connection, argsObject, commandName);

        int sendCountBefore = AlertSender.getSendCount();

        manager.run();

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }

    @Test
    public void testAfterFinishFailure() throws Exception {
        String commandName = "assign";
        String[] args = {commandName, "localhost", "balancer", "invalid",
                "--" + Args.OPTION_AFTER_FINISH + "=" + AlertSenderTest.ALERT_SCRIPT};
        Args argsObject = Manager.parseArgs(args);
        Manager manager = new Manager(connection, argsObject, commandName);

        int sendCountBefore = AlertSender.getSendCount();

        try {
            manager.run();
            Assert.fail();
        } catch (IllegalArgumentException ignore) {
        }

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }

    @Test
    public void testAfterFinishSuccess() throws Exception {
        String commandName = "assign";
        String[] args = {commandName, "localhost", "balancer", "on",
                "--" + Args.OPTION_AFTER_FINISH + "=" + AlertSenderTest.ALERT_SCRIPT};
        Args argsObject = Manager.parseArgs(args);
        Manager manager = new Manager(connection, argsObject, commandName);

        int sendCountBefore = AlertSender.getSendCount();

        manager.run();

        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }
}
