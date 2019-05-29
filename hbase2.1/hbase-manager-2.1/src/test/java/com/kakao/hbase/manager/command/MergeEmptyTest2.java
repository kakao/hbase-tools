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

public class MergeEmptyTest2 extends MergeTestBase {
    public MergeEmptyTest2() {
        super(MergeEmptyTest2.class);
    }

    @Test
    public void testMergeEmpty4() throws Exception {
        makeTestData4();

        List<RegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "empty", "--force-proceed", "--test"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
    }

    @Test
    public void testMergeEmpty5() throws Exception {
        makeTestData5();

        List<RegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "emptY", "--force-proceed", "--test"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.setTest(true);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
        regionInfoList = getRegionInfoList(tableName);
        // fixme
        assertEquals(2, regionInfoList.size());
        assertArrayEquals("".getBytes(), regionInfoList.get(0).getStartKey());
        assertArrayEquals("b".getBytes(), regionInfoList.get(1).getStartKey());
    }

    @Test
    public void testMergeEmpty6() throws Exception {
        makeTestData6();

        List<RegionInfo> regionInfoList;

        // merge
        String[] argsParam = {"zookeeper", tableName.getNameAsString(), "empty", "--force-proceed", "--max-iteration=4", "--test"};
        Args args = new ManagerArgs(argsParam);
        Merge command = new Merge(admin, args);
        command.run();
        Thread.sleep(Constant.WAIT_INTERVAL_MS);

        // check
        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());
    }
}
