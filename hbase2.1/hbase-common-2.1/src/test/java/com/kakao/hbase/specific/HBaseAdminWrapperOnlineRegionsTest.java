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
import org.junit.Assert;
import org.junit.Test;

public class HBaseAdminWrapperOnlineRegionsTest extends TestBase {
    public HBaseAdminWrapperOnlineRegionsTest() {
        super(HBaseAdminWrapperOnlineRegionsTest.class);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetOnlineRegions() throws Exception {
        createAdditionalTable(tableName + "2");

        int regionCount = 0;
        for (ServerName serverName : admin.getClusterStatus().getServers()) {
            for (HRegionInfo hRegionInfo : admin.getOnlineRegions(serverName)) {
                System.out.println(hRegionInfo);
                regionCount++;
            }
        }

        if (miniCluster) {
            if (securedCluster) {
                Assert.assertEquals(5, regionCount);
            } else {
                Assert.assertEquals(4, regionCount);
            }
        }
    }
}
