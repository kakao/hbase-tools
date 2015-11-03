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

package com.kakao.hbase.specific;

import com.kakao.hbase.TestBase;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CommandAdaptorTest extends TestBase {
    public CommandAdaptorTest() {
        super(CommandAdaptorTest.class);
    }

    @Test
    public void testGetOnlineRegions() throws Exception {
        splitTable("a".getBytes());
        splitTable("b".getBytes());

        ArrayList<ServerName> serverNameList = getServerNameList();
        assertEquals(TestBase.RS_COUNT, serverNameList.size());

        List<HRegionInfo> onlineRegions = CommandAdapter.getOnlineRegions(admin, serverNameList.get(0));
        assertEquals(3, onlineRegions.size());
    }
}
