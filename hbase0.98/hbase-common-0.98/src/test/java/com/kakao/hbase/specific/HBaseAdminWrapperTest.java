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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.junit.Assert;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;

public class HBaseAdminWrapperTest extends TestBase {
    public HBaseAdminWrapperTest() {
        super(HBaseAdminWrapperTest.class);
    }

    @Test
    public void testListTablesAsSuperUser() throws Exception {
        createAdditionalTable(tableName + "2");

        int tableCount = 0;
        for (HTableDescriptor hTableDescriptor : admin.listTables()) {
            System.out.println(hTableDescriptor);
            tableCount++;
        }

        if (miniCluster) {
            Assert.assertEquals(2, tableCount);
        }
    }

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

    @Test
    public void testGetOnlineRegionsWithOriginalHBaseAdmin() throws Exception {
        createAdditionalTable(tableName + "2");

        HBaseAdmin admin;
        if (securedCluster) {
            admin = new HBaseAdmin(conf);
        } else {
            admin = HBaseAdminWrapperTest.admin;
        }
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

    @Test
    public void testListTablesAsRWUser() throws Exception {
        createAdditionalTable(tableName + "2");

        final HTableDescriptor[] hTableDescriptors;
        if (securedCluster) {
            PrivilegedExceptionAction listTables = new PrivilegedExceptionAction() {
                @Override
                public Object run() throws Exception {
                    return admin.listTables();
                }
            };
            hTableDescriptors = (HTableDescriptor[]) USER_RW.runAs(listTables);
        } else {
            hTableDescriptors = admin.listTables();
        }

        int tableCount = 0;
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor);
            tableCount++;
        }

        if (miniCluster) {
            Assert.assertEquals(2, tableCount);
        }
    }

    @Test
    public void testListTablesAsRWUserWithOriginalHBaseAdmin() throws Exception {
        createAdditionalTable(tableName + "2");

        final HTableDescriptor[] hTableDescriptors;
        if (securedCluster) {
            PrivilegedExceptionAction listTables = new PrivilegedExceptionAction() {
                @Override
                public Object run() throws Exception {
                    try {
                        return new HBaseAdmin(conf).listTables();
                    } catch (AccessDeniedException e) {
                        return new HTableDescriptor[0];
                    }
                }
            };
            hTableDescriptors = (HTableDescriptor[]) USER_RW.runAs(listTables);
        } else {
            hTableDescriptors = new HBaseAdmin(conf).listTables();
        }

        int tableCount = 0;
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor);
            tableCount++;
        }

        if (miniCluster) {
            if (securedCluster)
                Assert.assertEquals(0, tableCount);
            else
                Assert.assertEquals(2, tableCount);
        }
    }
}
