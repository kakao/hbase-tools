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

import com.kakao.hbase.common.InvalidTableException;
import joptsimple.OptionException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Assert;
import org.junit.Test;

public class ArgsTest extends StatTestBase {
    public ArgsTest() {
        super(ArgsTest.class);
    }

    @Test
    public void testDisabledTable() throws Exception {
        try (Admin admin = connection.getAdmin()) {
            admin.disableTable(tableName);
            try {
                validateTable(tableName);
            } catch (InvalidTableException e) {
                if (!e.getMessage().contains("Table is not enabled")) {
                    throw e;
                }
            }
        }
    }

    @Test
    public void testExistingTable() throws Exception {
        validateTable(tableName);
    }

    @Test
    public void testNotExistingTable() throws Exception {
        try {
            validateTable(TableName.valueOf(tableName + "2"));
        } catch (InvalidTableException e) {
            if (!e.getMessage().contains("Table does not exist")) {
                throw e;
            }
        }
    }

    @Test
    public void testInvalidOption() throws Exception {
        String[] args = {"zookeeper", "table", "--nonono"};
        try {
            new StatArgs(args);
            Assert.fail();
        } catch (OptionException ignored) {
        }
    }

    @Test
    public void testNoArg() throws Exception {
        String[] args = {};
        try {
            new StatArgs(args);
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testNamespace() throws Exception {
        try (Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(TEST_NAMESPACE, tableName.getNameAsString());

            HTableDescriptor td = new HTableDescriptor(tn);
            HColumnDescriptor cd = new HColumnDescriptor(TEST_TABLE_CF);
            td.addFamily(cd);

            admin.createTable(td);

            validateTable(TableName.valueOf(TEST_NAMESPACE + ":" + tableName));

            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
    }
}
