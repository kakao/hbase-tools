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
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MergeEmptyFastTest extends MergeTestBase {
    public MergeEmptyFastTest() {
        super(MergeEmptyFastTest.class);
    }

    @Test
    public void testMergeEmptyFast1() throws Exception {
        makeTestData1();

        List<RegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "empty-FAST", "--force-proceed", "--test"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
    }

    @Test
    public void testMergeEmptyFast2() throws Exception {
        makeTestData2();

        List<RegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "empty-FAST", "--force-proceed", "--test"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("a".getBytes(), regionInfoList.get(1).getStartKey());
    }

    @Test
    public void testMergeEmptyFast3() throws Exception {
        makeTestData3();

        List<RegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "empty-FAST", "--force-proceed", "--test"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();

        // check
        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("b".getBytes(), regionInfoList.get(1).getStartKey());
        assertArrayEquals("c".getBytes(), regionInfoList.get(2).getStartKey());
    }
}
