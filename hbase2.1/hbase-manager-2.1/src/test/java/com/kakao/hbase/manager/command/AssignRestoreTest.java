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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AssignRestoreTest extends TestBase {
    public AssignRestoreTest() {
        super(AssignRestoreTest.class);
    }

    @Test
    public void testRestoreTable() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;
        ServerName rs1, rs2;
        String[] argsParam;
        Args args;
        Assign command;

        TableName tableName2 = createAdditionalTable(tableName + "2");
        splitTable("a".getBytes());
        splitTable("b".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(3, regionInfoList.size());

        Thread.sleep(10000);
        serverNameList = getServerNameList();
        rs1 = serverNameList.get(0);
        rs2 = serverNameList.get(1);

        // move all regions to rs1
        for (RegionInfo hRegionInfo : getRegionInfoList(tableName))
            move(hRegionInfo, rs1);
        move(getRegionInfoList(tableName2).get(0), rs1);
        assertEquals(3, getRegionInfoList(rs1, tableName).size());
        assertEquals(0, getRegionInfoList(rs2, tableName).size());
        assertEquals(1, getRegionInfoList(rs1, tableName2).size());
        assertEquals(0, getRegionInfoList(rs2, tableName2).size());
        String timestamp1 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        // move all regions to rs2 for tableName only
        for (RegionInfo hRegionInfo : getRegionInfoList(tableName))
            move(hRegionInfo, rs2);
        assertEquals(0, getRegionInfoList(rs1, tableName).size());
        assertEquals(3, getRegionInfoList(rs2, tableName).size());
        assertEquals(1, getRegionInfoList(rs1, tableName2).size());
        assertEquals(0, getRegionInfoList(rs2, tableName2).size());
        String timestamp2 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        // move one region to rs1, two regions to rs2 for tableName only
        move(getRegionInfoList(tableName).get(0), rs1);
        assertEquals(1, getRegionInfoList(rs1, tableName).size());
        assertEquals(2, getRegionInfoList(rs2, tableName).size());
        assertEquals(1, getRegionInfoList(rs1, tableName2).size());
        assertEquals(0, getRegionInfoList(rs2, tableName2).size());
        String timestamp3 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);

            // restore table with timestamp1
            argsParam = new String[]{"zookeeper", "restoRe", "tAble", tableName.getNameAsString(), timestamp1, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(3, getRegionInfoList(rs1, tableName).size());
            assertEquals(0, getRegionInfoList(rs2, tableName).size());
            assertEquals(1, getRegionInfoList(rs1, tableName2).size());
            assertEquals(0, getRegionInfoList(rs2, tableName2).size());
            assertEquals(2, AssignAction.getProcessedCount());

            // restore table with timestamp2
            argsParam = new String[]{"zookeeper", "restoRe", "table", tableName.getNameAsString(), timestamp2, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
            assertEquals(3, getRegionInfoList(rs2, tableName).size());
            assertEquals(1, getRegionInfoList(rs1, tableName2).size());
            assertEquals(0, getRegionInfoList(rs2, tableName2).size());
            assertEquals(3, AssignAction.getProcessedCount());

            // restore table with timestamp3
            argsParam = new String[]{"zookeeper", "restoRe", "table", tableName.getNameAsString(), timestamp3, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(1, getRegionInfoList(rs1, tableName).size());
            assertEquals(2, getRegionInfoList(rs2, tableName).size());
            assertEquals(1, getRegionInfoList(rs1, tableName2).size());
            assertEquals(0, getRegionInfoList(rs2, tableName2).size());
            assertEquals(1, AssignAction.getProcessedCount());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testRestoreTableException() throws Exception {
        List<RegionInfo> regionInfoList;
        String[] argsParam;
        Args args;
        Assign command;

        String timestamp1 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        splitTable("a".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());

        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);

            // restore table with timestamp1
            argsParam = new String[]{"zookeeper", "restoRe", "table", tableName.getNameAsString(), timestamp1, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();

            assertEquals(0, AssignAction.getProcessedCount());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testRestoreTableRegex() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;
        ServerName rs1, rs2;
        String[] argsParam;
        Args args;
        Assign command;

        TableName tableName2 = createAdditionalTable(tableName + "2");
        splitTable("a".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());

        serverNameList = getServerNameList();
        rs1 = serverNameList.get(0);
        rs2 = serverNameList.get(1);

        // move all regions to rs1
        move(getRegionInfoList(tableName).get(0), rs1);
        move(getRegionInfoList(tableName).get(1), rs1);
        move(getRegionInfoList(tableName2).get(0), rs1);
        assertEquals(2, getRegionInfoList(rs1, tableName).size());
        assertEquals(0, getRegionInfoList(rs2, tableName).size());
        assertEquals(1, getRegionInfoList(rs1, tableName2).size());
        assertEquals(0, getRegionInfoList(rs2, tableName2).size());
        String timestamp1 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        // move all regions to rs2
        move(getRegionInfoList(tableName).get(0), rs2);
        move(getRegionInfoList(tableName).get(1), rs2);
        move(getRegionInfoList(tableName2).get(0), rs2);
        assertEquals(0, getRegionInfoList(rs1, tableName).size());
        assertEquals(2, getRegionInfoList(rs2, tableName).size());
        assertEquals(0, getRegionInfoList(rs1, tableName2).size());
        assertEquals(1, getRegionInfoList(rs2, tableName2).size());
        String timestamp2 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String regex = tableName + ".*";

            // restore table with timestamp1
            argsParam = new String[]{"zookeeper", "restoRe", "table", regex, timestamp1, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(2, getRegionInfoList(rs1, tableName).size());
            assertEquals(0, getRegionInfoList(rs2, tableName).size());
            assertEquals(1, getRegionInfoList(rs1, tableName2).size());
            assertEquals(0, getRegionInfoList(rs2, tableName2).size());
            assertEquals(3, AssignAction.getProcessedCount());

            // restore table with timestamp2
            argsParam = new String[]{"zookeeper", "restoRe", "table", regex, timestamp2, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
            assertEquals(2, getRegionInfoList(rs2, tableName).size());
            assertEquals(0, getRegionInfoList(rs1, tableName2).size());
            assertEquals(1, getRegionInfoList(rs2, tableName2).size());
            assertEquals(3, AssignAction.getProcessedCount());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testRestoreSingleRs() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;
        ServerName rs1, rs2;
        String[] argsParam;
        Args args;
        Assign command;

        TableName tableName2 = createAdditionalTable(tableName + "2");
        splitTable("a".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());

        serverNameList = getServerNameList();
        rs1 = serverNameList.get(0);
        rs2 = serverNameList.get(1);

        // set case 1
        move(getRegionInfoList(tableName).get(0), rs1);
        move(getRegionInfoList(tableName).get(1), rs2);
        move(getRegionInfoList(tableName2).get(0), rs1);
        assertEquals(1, getRegionInfoList(rs1, tableName).size());
        assertEquals(1, getRegionInfoList(rs2, tableName).size());
        assertEquals(1, getRegionInfoList(rs1, tableName2).size());
        assertEquals(0, getRegionInfoList(rs2, tableName2).size());
        String timestamp1 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        // set case 2
        move(getRegionInfoList(tableName).get(0), rs2);
        move(getRegionInfoList(tableName).get(1), rs2);
        move(getRegionInfoList(tableName2).get(0), rs2);
        assertEquals(0, getRegionInfoList(rs1, tableName).size());
        assertEquals(2, getRegionInfoList(rs2, tableName).size());
        assertEquals(0, getRegionInfoList(rs1, tableName2).size());
        assertEquals(1, getRegionInfoList(rs2, tableName2).size());
        String timestamp2 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String regex = rs1.getHostname() + "," + rs1.getPort() + ".*";

            // restore table with timestamp1
            argsParam = new String[]{"zookeeper", "restoRe", "rS", regex, timestamp1, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(1, getRegionInfoList(rs1, tableName).size());
            assertEquals(1, getRegionInfoList(rs2, tableName).size());
            assertEquals(1, getRegionInfoList(rs1, tableName2).size());
            assertEquals(0, getRegionInfoList(rs2, tableName2).size());
            assertTrue(AssignAction.getProcessedCount() >= 2);

            // restore table with timestamp2
            argsParam = new String[]{"zookeeper", "restoRe", "rS", regex, timestamp2, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
            assertEquals(2, getRegionInfoList(rs2, tableName).size());
            assertEquals(0, getRegionInfoList(rs1, tableName2).size());
            assertEquals(1, getRegionInfoList(rs2, tableName2).size());
            assertTrue(AssignAction.getProcessedCount() >= 2);
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }

    @Test
    public void testRestoreRsRegex() throws Exception {
        ArrayList<ServerName> serverNameList;
        List<RegionInfo> regionInfoList;
        ServerName rs1, rs2;
        String[] argsParam;
        Args args;
        Assign command;

        TableName tableName2 = createAdditionalTable(tableName + "2");
        splitTable("a".getBytes());
        regionInfoList = getRegionInfoList(tableName);
        assertEquals(2, regionInfoList.size());

        serverNameList = getServerNameList();
        rs1 = serverNameList.get(0);
        rs2 = serverNameList.get(1);

        // set case 1
        move(getRegionInfoList(tableName).get(0), rs1);
        move(getRegionInfoList(tableName).get(1), rs2);
        move(getRegionInfoList(tableName2).get(0), rs1);
        assertEquals(1, getRegionInfoList(rs1, tableName).size());
        assertEquals(1, getRegionInfoList(rs2, tableName).size());
        assertEquals(1, getRegionInfoList(rs1, tableName2).size());
        assertEquals(0, getRegionInfoList(rs2, tableName2).size());
        String timestamp1 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        // set case 2
        move(getRegionInfoList(tableName).get(0), rs2);
        move(getRegionInfoList(tableName).get(1), rs2);
        move(getRegionInfoList(tableName2).get(0), rs2);
        assertEquals(0, getRegionInfoList(rs1, tableName).size());
        assertEquals(2, getRegionInfoList(rs2, tableName).size());
        assertEquals(0, getRegionInfoList(rs1, tableName2).size());
        assertEquals(1, getRegionInfoList(rs2, tableName2).size());
        String timestamp2 = Constant.DATE_FORMAT_ARGS.format(System.currentTimeMillis());

        boolean balancerRunning = false;
        try {
            balancerRunning = admin.balancerSwitch(false, true);
            String regex = rs1.getHostname() + ".*";

            // restore table with timestamp1
            argsParam = new String[]{"zookeeper", "restoRe", "rS", regex, timestamp1, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(1, getRegionInfoList(rs1, tableName).size());
            assertEquals(1, getRegionInfoList(rs2, tableName).size());
            assertEquals(1, getRegionInfoList(rs1, tableName2).size());
            assertEquals(0, getRegionInfoList(rs2, tableName2).size());

            // restore table with timestamp2
            argsParam = new String[]{"zookeeper", "restoRe", "rS", regex, timestamp2, "--force-proceed"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertEquals(0, getRegionInfoList(rs1, tableName).size());
            assertEquals(2, getRegionInfoList(rs2, tableName).size());
            assertEquals(0, getRegionInfoList(rs1, tableName2).size());
            assertEquals(1, getRegionInfoList(rs2, tableName2).size());
        } finally {
            if (balancerRunning)
                admin.balancerSwitch(true, true);
        }
    }
}
