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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class RegionLocationCleaner implements Runnable {
    public static final Object LOCK = new Object();
    public static final int THREAD_POOL_SIZE = 5;
    private static HConnection connection = null;
    private final String tableName;
    private final NavigableMap<HRegionInfo, ServerName> regionServerMap;

    public RegionLocationCleaner(String tableName, Configuration conf, NavigableMap<HRegionInfo, ServerName> regionServerMap) throws IOException {
        this.tableName = tableName;
        if (connection == null) {
            connection = HConnectionManager.createConnection(conf);
        }
        this.regionServerMap = regionServerMap;
    }

    @Override
    public void run() {
        try (HTableInterface table = connection.getTable(tableName.getBytes())) {
            // Do not use Get not to increase read request count metric.
            // Use Scan.
            Scan scan = new Scan("".getBytes(), "".getBytes());
            FilterList filterList = new FilterList();
            filterList.addFilter(new KeyOnlyFilter());
            filterList.addFilter(new FirstKeyOnlyFilter());
            scan.setFilter(filterList);
            //noinspection EmptyTryBlock
            try(ResultScanner ignored = table.getScanner(scan)) {
            }
            return;
        } catch (IOException ignore) {
        }

        clean(tableName);
    }

    private void clean(String tableName) {
        synchronized (LOCK) {
            List<HRegionInfo> toRemoveList = new ArrayList<>();
            for (HRegionInfo hRegionInfo : regionServerMap.keySet()) {
                if (CommandAdapter.getTableName(hRegionInfo).equals(tableName)) {
                    toRemoveList.add(hRegionInfo);
                }
            }
            for (HRegionInfo hRegionInfo : toRemoveList) {
                regionServerMap.remove(hRegionInfo);
            }
        }
    }
}
