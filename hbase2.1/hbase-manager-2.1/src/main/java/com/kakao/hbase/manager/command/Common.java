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

import com.google.common.annotations.VisibleForTesting;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Common {
    private Common() {
    }

    @VisibleForTesting
    static void move(Args args, Admin admin, TableName tableName, String targetServerName, String encodedRegionName,
        boolean asynchronous)
        throws IOException, InterruptedException {
        int i;
        for (i = 0; i < Constant.TRY_MAX; i++) {
            try {
                admin.move(encodedRegionName.getBytes(), targetServerName.getBytes());
            } catch (java.lang.reflect.UndeclaredThrowableException ignore) {
            } catch (DoNotRetryIOException e) {
                if (!e.getMessage().contains("is not OPEN"))
                    throw e;
            }

            if (asynchronous)
                return;

            if (CommandAdapter.isMetaTable(tableName))
                return;

            if (!isTableEnabled(args, admin, tableName))
                throw new IllegalStateException(Constant.MESSAGE_DISABLED_OR_NOT_FOUND_TABLE);

            if (Util.isMoved(admin.getConnection(), tableName, encodedRegionName, targetServerName)) {
                return;
            }

            Thread.sleep(Constant.WAIT_INTERVAL_MS);

            // assign region again
            if (i >= Constant.TRY_MAX / 2)
                admin.assign(encodedRegionName.getBytes());
        }
        if (i >= Constant.TRY_MAX)
            throw new IllegalStateException(Constant.MESSAGE_CANNOT_MOVE + " - "
                    + encodedRegionName + " to " + targetServerName);
    }

    static boolean isTableEnabled(Args args, Admin admin, TableName tableName)
        throws InterruptedException, IOException {
        long startTimestamp = System.currentTimeMillis();
        int i;

        boolean tableEnabled = false;
        for (i = 0; i < Constant.TRY_MAX; i++) {
            try {
                Util.printVerboseMessage(args, "isTableEnabled - iteration - " + i + " - start");
                tableEnabled = admin.isTableEnabled(tableName);
                Util.printVerboseMessage(args, "isTableEnabled - iteration - " + i + " - end", startTimestamp);
                break;
            } catch (TableNotFoundException e) {
                break;
            } catch (Exception e) {
                if (!e.getMessage().contains(NotServingRegionException.class.getSimpleName()))
                    throw e;
            }

            Thread.sleep(Constant.WAIT_INTERVAL_MS);
        }
        return tableEnabled;
    }

    static void moveWithPrintingResult(Args args, Admin admin, TableName tableName, String encodedRegionName,
        String serverNameDest, boolean asynchronous) throws IOException, InterruptedException {
        try {
            move(args, admin, tableName, serverNameDest, encodedRegionName, asynchronous);
            System.out.println(" - OK" + (asynchronous ? " - ASYNC" : ""));
        } catch (IllegalStateException e) {
            if (e.getMessage().contains(Constant.MESSAGE_DISABLED_OR_NOT_FOUND_TABLE)) {
                System.out.println(" - SKIPPED - " + Constant.MESSAGE_DISABLED_OR_NOT_FOUND_TABLE);
            } else {
                throw e;
            }
        } catch (UnknownRegionException e) {
            System.out.println(" - SKIPPED - " + e.getClass().getCanonicalName());
        }
    }

    /**
     * @return key: serverName exclude timestamp
     */
    static Map<String, ServerName> serverNameMap(Admin admin) throws IOException {
        Map<String, ServerName> serverNameMap = new HashMap<>();
        for (ServerName serverName : admin.getClusterStatus().getServers()) {
            serverNameMap.put(getServerNameKey(serverName.getServerName()), serverName);
        }
        return serverNameMap;
    }

    static String getServerNameKey(String serverNameOrg) {
        return serverNameOrg.split(",")[0] + "," + serverNameOrg.split(",")[1];
    }

    static List<ServerName> regionServers(Admin admin) throws IOException {
        return new ArrayList<>(admin.getClusterStatus().getServers());
    }

    static List<ServerName> regionServers(Admin admin, String regex) throws IOException {
        List<ServerName> result = new ArrayList<>();
        for (ServerName serverName : regionServers(admin)) {
            if (serverName.getServerName().matches(regex)) result.add(serverName);
        }
        return result;
    }
}
