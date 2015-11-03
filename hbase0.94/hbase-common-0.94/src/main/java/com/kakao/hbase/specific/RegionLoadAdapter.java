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

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.common.Util;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * For HBase 0.94
 */
public class RegionLoadAdapter {
    public static final LoadEntry[] loadEntries = RegionLoadDelegator.loadEntries();
    private final Map<HRegionInfo, RegionLoadDelegator> regionLoadMap = new HashMap<>();

    public RegionLoadAdapter(HBaseAdmin admin, Map<byte[], HRegionInfo> regionMap, Args args) throws IOException {
        long timestamp = System.currentTimeMillis();

        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> serverNames = clusterStatus.getServers();
        for (ServerName serverName : serverNames) {
            HServerLoad serverLoad = clusterStatus.getLoad(serverName);
            for (Map.Entry<byte[], HServerLoad.RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                if (regionMap.get(entry.getKey()) != null)
                    regionLoadMap.put(regionMap.get(entry.getKey()), new RegionLoadDelegator(entry.getValue()));
            }
        }

        Util.printVerboseMessage(args, "RegionLoadAdapter", timestamp);
    }

    public static int loadEntryOrdinal(LoadEntry loadEntry) {
        return Arrays.asList(loadEntries).indexOf(loadEntry);
    }

    public RegionLoadDelegator get(HRegionInfo hRegionInfo) {
        return regionLoadMap.get(hRegionInfo);
    }
}
