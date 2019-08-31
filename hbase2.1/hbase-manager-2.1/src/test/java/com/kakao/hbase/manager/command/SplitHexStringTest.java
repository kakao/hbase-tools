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
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SplitHexStringTest extends TestBase {
    public SplitHexStringTest() {
        super(SplitHexStringTest.class);
    }

    @Test
    public void testSplitRuleRegex() throws Exception {
        TableName tableName2 = createAdditionalTable(tableName + "2");

        // split
        String tableNameRegex = tableName2 + ".*";
        String[] argsParam = {"zookeeper", tableNameRegex, "Rule", "hexString_down", "3", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        assertEquals("zookeeper", args.getZookeeperQuorum());
        Command command = new Split(admin, args);

        command.run();
        waitForSplitting(tableName2, 3);

        assertEquals(1, getRegionInfoList(tableName).size());
        assertEquals(3, getRegionInfoList(tableName2).size());
    }

    @Test
    public void testSplitWithHexStringDowncaseRule() throws Exception {
        List<RegionInfo> regionInfoList;

        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "Rule", "hexString_down", "5", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        assertEquals("zookeeper", args.getZookeeperQuorum());
        Split command = new Split(admin, args);

        command.run();
        waitForSplitting(5);

        regionInfoList = getRegionInfoList(tableName);
        assertEquals(5, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList) {
            byte[] startKey = hRegionInfo.getStartKey();
            if (startKey.length > 0) {
                assertTrue(Bytes.toString(startKey).matches("[a-z0-9]*"));
                assertFalse(Bytes.toString(startKey).matches("[A-Z]*"));
            }
        }
    }

    @Test
    public void testSplitWithHexStringUpcaseRule() throws Exception {
        List<RegionInfo> regionInfoList;

        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());

        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "Rule", "hexString_up", "5", "--force-proceed"};
        Args args = new ManagerArgs(argsParam);
        assertEquals("zookeeper", args.getZookeeperQuorum());
        Split command = new Split(admin, args);

        command.run();
        waitForSplitting(5);

        regionInfoList = getRegionInfoList(tableName);
        assertEquals(5, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList) {
            byte[] startKey = hRegionInfo.getStartKey();
            if (startKey.length > 0) {
                assertTrue(Bytes.toString(startKey).matches("[A-Z0-9]*"));
                assertFalse(Bytes.toString(startKey).matches("[a-z]*"));
            }
        }
    }
}
