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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AssignImportTest extends TestBase {
    public AssignImportTest() {
        super(AssignImportTest.class);
    }

    @Test
    public void testImportAsync() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;
        ServerName rs1, rs2;
        List<String> assignmentList;

        String expFileName = "export_test.exp";

        splitTable("a".getBytes());
        splitTable("b".getBytes());

        serverNameList = getServerNameList();
        rs1 = serverNameList.get(0);
        rs2 = serverNameList.get(1);

        // move all regions to rs1
        regionInfoList = getRegionInfoList(tableName);
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);
        assertEquals(regionInfoList.size(), getRegionInfoList(rs1, tableName).size());

        boolean balancerRunning = false;
        try {
            String[] argsParam;
            Args args;
            Assign command;

            // export
            balancerRunning = admin.balancerSwitch(false, true);
            argsParam = new String[]{"zookeeper", "export", expFileName};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            assignmentList = AssignTest.readExportFile(expFileName);
            assertEquals(AssignAction.getProcessedCount(), assignmentList.size());

            // remember region count of rs1
            int regionCountRS1 = getRegionInfoList(rs1, tableName).size();

            // move all regions to rs2
            regionInfoList = getRegionInfoList(tableName);
            for (RegionInfo hRegionInfo : regionInfoList)
                move(hRegionInfo, rs2);
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
            assertEquals(regionInfoList.size(), getRegionInfoList(rs2, tableName).size());

            // import
            balancerRunning = admin.balancerSwitch(false, true);
            argsParam = new String[]{"zookeeper", "import", expFileName, "--force-proceed", "--move-async"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            // verify
            assertEquals(regionCountRS1, getRegionInfoList(rs1, tableName).size());
            assertEquals(regionInfoList.size(), getRegionInfoList(tableName).size());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
            Files.delete(Paths.get(expFileName));
        }
    }

    @Test
    public void testImportWithRS() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;
        ServerName rs1, rs2;

        String expFileName = "export_test.exp";

        splitTable("a".getBytes());
        splitTable("b".getBytes());

        serverNameList = getServerNameList();
        rs1 = serverNameList.get(0);
        rs2 = serverNameList.get(1);

        // move all regions to rs1 except 1
        regionInfoList = getRegionInfoList(tableName);
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);
        RegionInfo region1 = regionInfoList.get(0);
        RegionInfo region2 = regionInfoList.get(1);
        RegionInfo region3 = regionInfoList.get(2);
        move(region1, rs2);
        assertEquals(region1, getRegionInfoList(rs2, tableName).get(0));
        assertEquals(region2, getRegionInfoList(rs1, tableName).get(0));
        assertEquals(region3, getRegionInfoList(rs1, tableName).get(1));

        boolean balancerRunning = false;
        try {
            String[] argsParam;
            Args args;
            Assign command;

            balancerRunning = admin.balancerSwitch(false, true);

            // export all RSs
            argsParam = new String[]{"zookeeper", "export", expFileName};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            // move regions
            move(region1, rs1);
            move(region2, rs2);
            move(region3, rs1);
            assertEquals(region1, getRegionInfoList(rs1, tableName).get(0));
            assertEquals(region2, getRegionInfoList(rs2, tableName).get(0));
            assertEquals(region3, getRegionInfoList(rs1, tableName).get(1));

            // import rs2
            String serverNameModified = rs2.getServerName().substring(0,
                rs2.getServerName().lastIndexOf(",")) + "," + System.currentTimeMillis();
            argsParam = new String[]{"zookeeper", "import", expFileName,
                "--force-proceed", "--rs=" + serverNameModified};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            // check
            assertEquals(region1, getRegionInfoList(rs2, tableName).get(0));
            assertEquals(region2, getRegionInfoList(rs2, tableName).get(1));
            assertEquals(region3, getRegionInfoList(rs1, tableName).get(0));
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
            Files.delete(Paths.get(expFileName));
        }
    }
}
