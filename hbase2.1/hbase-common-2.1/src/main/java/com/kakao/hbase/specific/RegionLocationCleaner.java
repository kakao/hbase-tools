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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class RegionLocationCleaner implements Runnable {
    private static final Object LOCK = new Object();
    public static final int THREAD_POOL_SIZE = 5;
    private final Connection connection;
    private final TableName tableName;
    private final NavigableMap<RegionInfo, ServerName> regionServerMap;

    public RegionLocationCleaner(TableName tableName, Connection connection, NavigableMap<RegionInfo, ServerName> regionServerMap) {
        this.tableName = tableName;
        this.connection = connection;
        this.regionServerMap = regionServerMap;
    }

    @Override
    public void run() {
        try (Table table = connection.getTable(tableName)) {
            // Do not use Get not to increase read request count metric.
            // Use Scan.
            Scan scan = new Scan().withStartRow("".getBytes()).withStopRow("".getBytes());
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

    private void clean(TableName tableName) {
        synchronized (LOCK) {
            List<RegionInfo> toRemoveList = new ArrayList<>();
            for (RegionInfo hRegionInfo : regionServerMap.keySet()) {
                if (CommandAdapter.getTableName(hRegionInfo).equals(tableName)) {
                    toRemoveList.add(hRegionInfo);
                }
            }
            for (RegionInfo hRegionInfo : toRemoveList) {
                regionServerMap.remove(hRegionInfo);
            }
        }
    }
}
