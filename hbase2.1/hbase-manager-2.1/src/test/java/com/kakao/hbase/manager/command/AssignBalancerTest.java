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
import com.kakao.hbase.specific.CommandAdapter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class AssignBalancerTest extends TestBase {
    public AssignBalancerTest() {
        super(AssignBalancerTest.class);
    }

    @Test
    public void testIsBalancerRunning() throws Exception {
        boolean balancerRunning = false;
        try {
            balancerRunning = admin.setBalancerRunning(true, true);
            assertTrue(CommandAdapter.isBalancerRunning(admin));
            admin.setBalancerRunning(false, true);
            assertFalse(CommandAdapter.isBalancerRunning(admin));
        } finally {
            if (balancerRunning)
                admin.setBalancerRunning(true, true);
        }
    }

    @Test
    public void testBalancer() throws Exception {
        boolean balancerRunning = false;
        try {
            balancerRunning = admin.setBalancerRunning(true, true);

            String[] argsParam;
            Args args;
            Assign command;

            argsParam = new String[]{"zookeeper", "balancer", "off"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertFalse(CommandAdapter.isBalancerRunning(admin));

            argsParam = new String[]{"zookeeper", "balancer", "on"};
            args = new ManagerArgs(argsParam);
            command = new Assign(admin, args);
            command.run();
            assertTrue(CommandAdapter.isBalancerRunning(admin));
        } finally {
            if (balancerRunning)
                admin.setBalancerRunning(true, true);
        }
    }
}
