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

package com.kakao.hbase.common;

import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class EmptyRegionChecker implements Runnable {
    private static final AtomicLong counter = new AtomicLong();
    public static int THREAD_POOL_SIZE = 10;
    private final Connection connection;
    private final String tableName;
    private final RegionInfo regionInfo;
    private final Set<RegionInfo> emptyRegions;

    public EmptyRegionChecker(Connection connection, String tableName,
                              RegionInfo regionInfo, Set<RegionInfo> emptyRegions) {
        this.connection = connection;
        this.tableName = tableName;
        this.regionInfo = regionInfo;
        this.emptyRegions = emptyRegions;
    }

    public static void resetCounter() {
        counter.set(0);
    }

    @Override
    public void run() {
        try {
            if (CommandAdapter.isReallyEmptyRegion(connection, tableName, regionInfo)) {
                emptyRegions.add(regionInfo);
                long count = counter.incrementAndGet();
                if (count % 100 == 0) {
                    Util.printMessage("Finding empty regions in progress - " + count + " empty regions");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
