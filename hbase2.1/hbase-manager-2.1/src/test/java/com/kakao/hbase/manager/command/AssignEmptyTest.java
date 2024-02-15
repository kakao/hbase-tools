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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AssignEmptyTest extends TestBase {
    public AssignEmptyTest() {
        super(AssignEmptyTest.class);
    }

    @Test
    public void testEmpty() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;

        // move all regions to rs1
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        serverNameList = getServerNameList();
        ServerName rs1 = serverNameList.get(0);
        ServerName rs2 = serverNameList.get(1);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);

        assertEquals(regionInfoList.size(), getRegionInfoList(rs1, tableName).size());

        // empty rs1
        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String[] argsParam = {"zookeeper", "empty", rs1.getServerName(), "--skip-export", "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);
            command.run();

            // verify
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }

        // empty rs2 with regex. single RS is emptied.
        balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String serverNameRegex = rs2.getServerName().substring(0, rs2.getServerName().lastIndexOf(',')) + ".*";
            String[] argsParam = {"zookeeper", "empty", serverNameRegex, "--skip-export", "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);
            command.run();

            // verify
            assertEquals(0, getRegionInfoList(rs2, tableName).size());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testEmptyAll() throws Exception {
        if (miniCluster) {
            // empty rs1
            boolean balancerRunning = false;
            try {
                balancerRunning = admin.balancerSwitch(false, true);
                String serverNameRegex = ".*";
                String[] argsParam = {"zookeeper", "empty", serverNameRegex, "--skip-export", "--force-proceed"};
                Args args = new ManagerArgs(argsParam);
                Assign command = new Assign(admin, args);
                try {
                    command.run();
                    fail();
                } catch (IllegalArgumentException e) {
                    if (!e.getMessage().contains("Cannot empty all RS"))
                        throw e;
                }
            } finally {
                if (balancerRunning)
                    admin.balancerSwitch(true, true);
            }
        }
    }

    @Test
    public void testEmptyWithBalancerOffOption() throws Exception {
        ArrayList<ServerName> serverNameList = getServerNameList();
        ServerName rs1 = serverNameList.get(0);

        boolean balancerRunning = false;

        // invalid
        try {
            balancerRunning = false;
            String[] argsParam = {"zookeeper", "empty", rs1.getServerName()};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);

            balancerRunning = admin.balancerSwitch(true, true);
            command.run();
            fail();
        } catch (IllegalStateException e) {
            if (!e.getMessage().contains(AssignAction.MESSAGE_TURN_BALANCER_OFF))
                throw e;
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }

        // valid
        try {
            balancerRunning = false;
            String[] argsParam = {"zookeeper", "empty", rs1.getServerName(), "--" + Args.OPTION_TURN_BALANCER_OFF,
                "--skip-export", "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);

            balancerRunning = admin.balancerSwitch(true, true);
            command.run();
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testEmptyAsync() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;

        // move all regions to rs1
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        serverNameList = getServerNameList();
        ServerName rs1 = serverNameList.get(0);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);

        assertEquals(regionInfoList.size(), getRegionInfoList(rs1, tableName).size());

        // empty rs1
        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String[] argsParam = {"zookeeper", "empty", rs1.getServerName(),
                "--force-proceed", "--move-async", "--skip-export"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);
            command.run();

            // verify
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testEmptyWithExport() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;

        // move all regions to rs1
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        serverNameList = getServerNameList();
        ServerName rs1 = serverNameList.get(0);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);

        assertEquals(regionInfoList.size(), getRegionInfoList(rs1, tableName).size());

        String expFileName = "export_test.exp";
        // empty rs1 with export
        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String[] argsParam = {"zookeeper", "empty", rs1.getServerName(), expFileName, "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);
            command.run();

            // check empty
            assertEquals(0, getRegionInfoList(rs1, tableName).size());

            // check export file
            List<String> assignmentList = AssignTest.readExportFile(expFileName);
            int actual = 0;
            for (String assignment : assignmentList) {
                for (RegionInfo hRegionInfo : regionInfoList) {
                    if (assignment.contains(hRegionInfo.getEncodedName()))
                        actual++;
                }
            }
            assertEquals(regionInfoList.size(), actual);
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testEmptySkipExport() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;

        // move all regions to rs1
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        serverNameList = getServerNameList();
        ServerName rs1 = serverNameList.get(0);
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);

        assertEquals(regionInfoList.size(), getRegionInfoList(rs1, tableName).size());

        // empty rs1 with export
        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String[] argsParam = {"zookeeper", "empty", rs1.getServerName(), "--skip-export", "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);
            command.run();

            // check empty
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }
}
