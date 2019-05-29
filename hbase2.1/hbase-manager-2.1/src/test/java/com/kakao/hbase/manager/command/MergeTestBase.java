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

package com.kakao.hbase.manager.command;

import com.kakao.hbase.TestBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;

import java.util.List;

import static org.junit.Assert.assertEquals;

class MergeTestBase extends TestBase {
    MergeTestBase(Class c) {
        super(c);
    }

    void makeTestData1() throws Exception {
        List<RegionInfo> regionInfoList;// split table to 3 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
    }

    void makeTestData2() throws Exception {
        List<RegionInfo> regionInfoList;// split table to 4 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(4, regionInfoList.size());

        // put data to the first region
        try (Table table = getTable(tableName)) {
            Put put = new Put("1".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    void makeTestData3() throws Exception {
        List<RegionInfo> regionInfoList;// split table to 4 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(4, regionInfoList.size());

        // put data to the second region
        try (Table table = getTable(tableName)) {
            Put put = new Put("b".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    void makeTestData4() throws Exception {
        List<RegionInfo> regionInfoList;// split table to 4 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(4, regionInfoList.size());

        // put data to the last region
        try (Table table = getTable(tableName)) {
            Put put = new Put("c".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    void makeTestData5() throws Exception {
        List<RegionInfo> regionInfoList;// split table to 5 regions
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        splitTable("d".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(5, regionInfoList.size());

        // put data to the second and forth region
        try (Table table = getTable(tableName)) {
            Put put;
            put = new Put("a".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
            put = new Put("c".getBytes());
            put.addColumn(TEST_TABLE_CF.getBytes(), "c1".getBytes(), "data".getBytes());
            table.put(put);
        }
    }

    void makeTestData6() throws Exception {
        List<RegionInfo> regionInfoList;// split table
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        splitTable("c".getBytes());
        splitTable("d".getBytes());
        splitTable("e".getBytes());
        splitTable("f".getBytes());
        splitTable("g".getBytes());
        splitTable("h".getBytes());
        splitTable("i".getBytes());
        splitTable("j".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(11, regionInfoList.size());
    }
}
