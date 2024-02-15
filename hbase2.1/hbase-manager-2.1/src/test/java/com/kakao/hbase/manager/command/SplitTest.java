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

import com.kakao.hbase.ManagerArgs;
import com.kakao.hbase.TestBase;
import com.kakao.hbase.common.Args;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SplitTest extends TestBase {
    public SplitTest() {
        super(SplitTest.class);
    }

    @Test
    public void testSplitWithFile() throws Exception {
        final String keyFileName = "exportkeys_test.keys";

        List<RegionInfo> regionInfoList;
        try {
            {
                // export
                splitTable("a".getBytes());
                splitTable("b".getBytes());
                waitForSplitting(3);

                String[] argsParam = {"zookeeper", tableName.getNameAsString(), keyFileName, "--force-proceed"};
                Args args = new ManagerArgs(argsParam);
                assertEquals("zookeeper", args.getZookeeperQuorum());
                ExportKeys command = new ExportKeys(admin, args);
                command.run();

                // merge all regions into one
                regionInfoList = getRegionInfoList(tableName);
                mergeRegion(regionInfoList.get(0), regionInfoList.get(1));
                regionInfoList = getRegionInfoList(tableName);
                mergeRegion(regionInfoList.get(0), regionInfoList.get(1));
                assertEquals(1, getRegionInfoList(tableName).size());
            }

            String[] argsParam = {"zookeeper", tableName.getNameAsString(), "filE", keyFileName, "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            assertEquals("zookeeper", args.getZookeeperQuorum());
            Split command = new Split(admin, args);

            command.run();
            waitForSplitting(3);

            regionInfoList = getRegionInfoList(tableName);
            assertEquals(3, regionInfoList.size());
        } finally {
            Files.delete(Paths.get(keyFileName));
        }
    }

    @Test
    public void testSplitWithDecimalString() throws Exception {
        List<RegionInfo> regionInfoList;

        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "Rule", "decimalString", "3", "10", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        assertEquals("zookeeper", args.getZookeeperQuorum());
        Split command = new Split(admin, args);

        command.run();
        waitForSplitting(3);

        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList) {
            byte[] startKey = hRegionInfo.getStartKey();
            if (startKey.length > 0) {
                assertTrue(Bytes.toString(startKey).matches("[0-9]*"));
                assertFalse(Bytes.toString(startKey).matches("[A-Za-z]*"));
            }
        }
    }

    @Test
    public void testSplitFileRegex() throws Exception {
        String keyFileName = "exportkeys_test.keys";

        List<RegionInfo> regionInfoList;

        byte[] splitPoint = "splitpoint".getBytes();
        splitTable(splitPoint);
        byte[] splitPoint2 = "splitpoint2".getBytes();
        TableName tableName2 = createAdditionalTable(tableName + "2");
        splitTable(tableName2, splitPoint2);
        byte[] splitPoint31 = Bytes.toBytes(3100L);
        byte[] splitPoint32 = Bytes.toBytes(3200L);
        TableName tableName3 = createAdditionalTable(tableName + "22");
        splitTable(tableName3, splitPoint31);
        splitTable(tableName3, splitPoint32);

        try {
            // export 3 tables
            String tableNameRegex = tableName + ".*";
            String[] argsParam = {"zookeeper", tableNameRegex, keyFileName};
            Args args = new ManagerArgs(argsParam);
            Assert.assertEquals("zookeeper", args.getZookeeperQuorum());
            Command command = new ExportKeys(admin, args);

            waitForSplitting(2);
            waitForSplitting(tableName2, 2);
            waitForSplitting(tableName3, 3);
            command.run();

            // merge all regions into one
            regionInfoList = getRegionInfoList(tableName);
            mergeRegion(regionInfoList.get(0), regionInfoList.get(1));
            assertEquals(1, getRegionInfoList(tableName).size());
            regionInfoList = getRegionInfoList(tableName2);
            mergeRegion(tableName2, regionInfoList.get(0), regionInfoList.get(1));
            assertEquals(1, getRegionInfoList(tableName2).size());
            regionInfoList = getRegionInfoList(tableName3);
            mergeRegion(tableName3, regionInfoList.get(0), regionInfoList.get(1));
            regionInfoList = getRegionInfoList(tableName3);
            mergeRegion(tableName3, regionInfoList.get(0), regionInfoList.get(1));
            assertEquals(1, getRegionInfoList(tableName3).size());

            // split
            tableNameRegex = tableName2 + ".*";
            String[] argsParam2 = {"zookeeper", tableNameRegex, "filE", keyFileName, "--force-proceed"};
            Args args2 = new ManagerArgs(argsParam2);
            assertEquals("zookeeper", args2.getZookeeperQuorum());
            command = new Split(admin, args2);

            command.run();
            waitForSplitting(tableName2, 2);
            waitForSplitting(tableName3, 3);

            ArrayList<RegionInfo> regionInfoList1 = getRegionInfoList(tableName);
            ArrayList<RegionInfo> regionInfoList2 = getRegionInfoList(tableName2);
            ArrayList<RegionInfo> regionInfoList3 = getRegionInfoList(tableName3);
            assertEquals(1, regionInfoList1.size());
            assertEquals(2, regionInfoList2.size());
            assertArrayEquals(splitPoint2, regionInfoList2.get(1).getStartKey());
            assertEquals(3, regionInfoList3.size());
            assertArrayEquals(splitPoint31, regionInfoList3.get(1).getStartKey());
            assertArrayEquals(splitPoint32, regionInfoList3.get(2).getStartKey());
        } finally {
            Path filePath = Paths.get(keyFileName);
            if (Files.exists(filePath)) Files.delete(filePath);
        }
    }
}
