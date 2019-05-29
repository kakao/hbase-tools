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

import com.kakao.hbase.specific.RegionLoadAdapter;
import com.kakao.hbase.TestBase;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

class StatTestBase extends TestBase {
    final int loadEntryLength = RegionLoadAdapter.loadEntries.length;

    StatTestBase(Class c) {
        super(c);
    }

    void putData() throws Exception {
        try (HTable table = (HTable) connection.getTable(tableName)) {
            Put put;
            put = new Put("0".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c".getBytes(), "0".getBytes());
            table.put(put);
            put = new Put("a".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c".getBytes(), "a".getBytes());
            table.put(put);
            put = new Put("b".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c".getBytes(), "b".getBytes());
            table.put(put);
        }
    }
}
