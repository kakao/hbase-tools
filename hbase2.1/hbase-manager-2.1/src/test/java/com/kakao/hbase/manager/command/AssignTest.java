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
import com.kakao.hbase.common.Constant;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class AssignTest extends TestBase {
    public AssignTest() {
        super(AssignTest.class);
    }

    @Test
    public void testExportImportWithBalancerOffOption() throws Exception {
        String expFileName = "export_test.exp";
        boolean balancerRunning = false;

        // invalid export
        try {
            balancerRunning = false;
            String[] argsParam = {"zookeeper", "export", expFileName};
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

        // valid export
        try {
            balancerRunning = false;
            String[] argsParam = {"zookeeper", "export", expFileName, "--" + Args.OPTION_TURN_BALANCER_OFF,
                "--skip-export", "--force-proceed"};
            Args args = new ManagerArgs(argsParam);
            Assign command = new Assign(admin, args);

            balancerRunning = admin.balancerSwitch(true, true);
            command.run();
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }

        // invalid import
        try {
            balancerRunning = false;
            String[] argsParam = {"zookeeper", "import", expFileName};
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

        // valid import
        try {
            balancerRunning = false;
            String[] argsParam = {"zookeeper", "import", expFileName, "--" + Args.OPTION_TURN_BALANCER_OFF,
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
    public void testExportImport() throws Exception {
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

            assignmentList = readExportFile(expFileName);
            assertEquals(AssignAction.getProcessedCount(), assignmentList.size());

            // modify exported file
            try (PrintWriter writer = new PrintWriter(expFileName, Constant.CHARSET.name())) {
                int i = 0, j = 0;
                for (String assignment : assignmentList) {
                    if (i == 0) {
                        // add an invalid region
                        writer.println(assignment.replaceAll("/.*/", "/nonono/"));
                    }

                    // add a not existing table
                    if (j == 0 && assignment.endsWith(tableName.getNameAsString())) {
                        writer.println(assignment.replaceAll("/" + tableName, "/" + tableName + "_INVALID"));
                        j++;
                    }

                    if (i == 2) {
                        // modify region server timestamp
                        int index1 = assignment.indexOf(",");
                        int index2 = assignment.indexOf(",", index1 + 1);
                        int index3 = assignment.indexOf(AssignAction.DELIMITER, index2 + 1);
                        String rsTimestamp = assignment.substring(index2 + 1, index3);

                        writer.println(assignment.replaceAll(rsTimestamp, String.valueOf(System.currentTimeMillis())));
                    } else {
                        writer.println(assignment);
                    }

                    i++;
                }
            }
            assignmentList = readExportFile(expFileName);
            assertEquals(AssignAction.getProcessedCount() + 2, assignmentList.size());

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
            argsParam = new String[]{"zookeeper", "import", expFileName, "--force-proceed"};
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
    public void testExportWithRS() throws Exception {
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

        // move all regions to rs1 except 1
        regionInfoList = getRegionInfoList(tableName);
        for (RegionInfo hRegionInfo : regionInfoList)
            move(hRegionInfo, rs1);
        RegionInfo firstRegion = regionInfoList.get(0);
        move(firstRegion, rs2);
        assertEquals(regionInfoList.size() - 1, getRegionInfoList(rs1, tableName).size());
        assertEquals(1, getRegionInfoList(rs2, tableName).size());

        boolean balancerRunning = false;
        try {
            String[] argsParam;
            Args args;
            Assign command;

            balancerRunning = admin.balancerSwitch(false, true);

            // export with full RS name
            argsParam = new String[]{"zookeeper", "export", expFileName, "--rs=" + rs1.getServerName()};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            assignmentList = readExportFile(expFileName);
            assertEquals(AssignAction.getProcessedCount(), assignmentList.size());
            for (String assignment : assignmentList) {
                assertFalse(firstRegion.getEncodedName() + " must not be exported.",
                    assignment.contains(firstRegion.getEncodedName()));
            }

            // export with regex RS name
            String regex = rs2.getServerName().substring(0, rs2.getServerName().indexOf(',')) + ".*";
            argsParam = new String[]{"zookeeper", "export", expFileName, "--rs=" + regex};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            assignmentList = readExportFile(expFileName);
            assertEquals(AssignAction.getProcessedCount(), assignmentList.size());
            int actual = 0;
            for (String assignment : assignmentList) {
                if (assignment.contains(firstRegion.getEncodedName()))
                    actual++;
            }
            assertEquals(firstRegion.getEncodedName() + " must be exported.", 1, actual);
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
            Files.delete(Paths.get(expFileName));
        }
    }

    @Test
    public void testExportRS1ImportRS2() throws Exception {
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

            // export RS1
            argsParam = new String[]{"zookeeper", "export", expFileName, "--rs=" + rs1.getServerName()};
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
            argsParam = new String[]{"zookeeper", "import", expFileName,
                "--force-proceed", "--rs=" + rs2.getServerName()};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            // check
            assertEquals(region1, getRegionInfoList(rs1, tableName).get(0));
            assertEquals(region2, getRegionInfoList(rs2, tableName).get(0));
            assertEquals(region3, getRegionInfoList(rs1, tableName).get(1));
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
            Files.delete(Paths.get(expFileName));
        }
    }

    static List<String> readExportFile(String expFileName) throws IOException {
        List<String> assignmentList;
        assignmentList = new ArrayList<>(Files.readAllLines(Paths.get(expFileName), Constant.CHARSET));
        return assignmentList;
    }

    @Test
    public void testMove() throws Exception {
        ArrayList<ServerName> serverNameList = getServerNameList();
        ArrayList<RegionInfo> regionInfoList = getRegionInfoList(tableName);
        assertEquals(1, regionInfoList.size());

        // move region to rs0
        move(regionInfoList.get(0), serverNameList.get(0));

        // fixme
        // split and move
        splitTable("a".getBytes());
        Common.move(null, admin, tableName, serverNameList.get(1).getServerName(),
            regionInfoList.get(0).getEncodedName(), false);
    }
}
