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

import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

class Common {
    private Common() {
    }

    @VisibleForTesting
    static void move(HBaseAdmin admin, String tableName, String targetServerName, String encodedRegionName, boolean asynchronous)
            throws IOException, InterruptedException {
        int i;
        for (i = 0; i < Constant.TRY_MAX; i++) {
            try {
                admin.move(encodedRegionName.getBytes(), targetServerName.getBytes());
            } catch (java.lang.reflect.UndeclaredThrowableException ignore) {
            }

            if (asynchronous)
                return;

            if (CommandAdapter.isMetaTable(tableName))
                return;

            if (!isTableEnabled(admin, tableName))
                throw new IllegalStateException(Constant.MESSAGE_DISABLED_OR_NOT_FOUND_TABLE);

            if (Util.isMoved(admin, tableName, encodedRegionName, targetServerName)) {
                return;
            }

            Thread.sleep(Constant.WAIT_INTERVAL_MS);

            // assign region again
            if (i >= Constant.TRY_MAX / 2)
                admin.assign(encodedRegionName.getBytes());
        }
        if (i >= Constant.TRY_MAX)
            throw new IllegalStateException(Constant.MESSAGE_CANNOT_MOVE + " - " + encodedRegionName + " to " + targetServerName);
    }

    static boolean isTableEnabled(HBaseAdmin admin, String tableName) throws InterruptedException, IOException {
        int i;

        boolean tableEnabled = false;
        for (i = 0; i < Constant.TRY_MAX; i++) {
            try {
                tableEnabled = admin.isTableEnabled(tableName);
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

    static void moveWithPrintingResult(HBaseAdmin admin, String tableName, String encodedRegionName, String serverNameDest, boolean asynchronous) throws IOException, InterruptedException {
        try {
            move(admin, tableName, serverNameDest, encodedRegionName, asynchronous);
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
}
