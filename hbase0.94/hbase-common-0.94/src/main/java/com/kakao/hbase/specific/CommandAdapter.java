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
import com.kakao.hbase.common.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest.*;

/**
 * For HBase 0.94
 */
public class CommandAdapter {
    private static NavigableMap<HRegionInfo, ServerName> hRegionInfoServerNameMap = null;
    private static long cachedTimestamp = System.currentTimeMillis();
    private static final long CACHE_TTL = 2000;

    private CommandAdapter() {
    }

    @SuppressWarnings("UnusedParameters")
    public static List<RegionPlan> makePlan(HBaseAdmin admin, Map<ServerName, List<HRegionInfo>> clusterState, Configuration conf) throws IOException {
        throw new IllegalStateException("This rule is not supported in hbase 0.94");
    }

    @SuppressWarnings("UnusedParameters")
    public static List<RegionPlan> makePlan(HBaseAdmin admin, List<RegionPlan> newRegionPlan) throws IOException {
        // should be empty list
        return new ArrayList<>();
    }

    public static List<HRegionInfo> getOnlineRegions(HBaseAdmin admin, ServerName serverName) throws IOException {
        List<HRegionInfo> hRegionInfoList = new ArrayList<>();

        // do not use admin.getTableNames(). It is not supported in 0.94.6.
        if (hRegionInfoServerNameMap == null || (System.currentTimeMillis() - cachedTimestamp) > CACHE_TTL) {
            hRegionInfoServerNameMap = regionServerMap(null, admin.getConfiguration(), null, false);
            cachedTimestamp = System.currentTimeMillis();
        }
        for (Map.Entry<HRegionInfo, ServerName> entry : hRegionInfoServerNameMap.entrySet()) {
            if (entry.getValue().equals(serverName))
                hRegionInfoList.add(entry.getKey());
        }

        return hRegionInfoList;
    }

    public static boolean isMetaTable(String tableName) {
        return tableName.equals("hbase:meta");
    }

    public static boolean isBalancerRunning(HBaseAdmin admin) throws IOException {
        boolean balancerRunning = admin.setBalancerRunning(false, false);
        if (balancerRunning) {
            admin.setBalancerRunning(true, false);
        }
        return balancerRunning;
    }

    public static Map<ServerName, List<HRegionInfo>> initializeRegionMap(HBaseAdmin admin) throws IOException {
        Map<ServerName, List<HRegionInfo>> regionMap = new TreeMap<>();
        for (ServerName serverName : admin.getClusterStatus().getServers())
            regionMap.put(serverName, new ArrayList<HRegionInfo>());
        return regionMap;
    }

    @SuppressWarnings("UnusedParameters")
    public static void loginUserFromSubject(Configuration conf, Subject subject) throws IOException {
        throw new IllegalArgumentException("You must use keytab on HBase 0.94.");
    }

    @SuppressWarnings("UnusedParameters")
    public static NavigableMap<HRegionInfo, ServerName> regionServerMap(Args args, Configuration conf, HConnection connection, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<HRegionInfo, ServerName> regions = new TreeMap<>();
        MetaScanner.BlockingMetaScannerVisitor visitor = new MetaScanner.BlockingMetaScannerVisitor(conf) {
            @Override
            public boolean processRowInternal(Result rowResult) throws IOException {
                HRegionInfo info = Writables.getHRegionInfo(rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
                byte[] value = rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
                String hostAndPort = null;
                if (value != null && value.length > 0) {
                    hostAndPort = Bytes.toString(value);
                }
                value = rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
                long startcode = -1L;
                if (value != null && value.length > 0) startcode = Bytes.toLong(value);
                if (!(info.isOffline() || info.isSplit())) {
                    ServerName sn = null;
                    if (hostAndPort != null && hostAndPort.length() > 0) {
                        sn = new ServerName(hostAndPort, startcode);
                    }
                    if (info.isOffline() && !offlined) return true;
                    regions.put(info, sn);
                }
                return true;
            }
        };
        MetaScanner.metaScan(conf, visitor);

        // add root region
        try(HTable table = new HTable(conf, "-ROOT-")) {
            Set<ServerName> serverNames = new HashSet<>();
            serverNames.addAll(regions.values());

            HRegionLocation rootRegionLocation = table.getRegionLocation("".getBytes(), true);
            for (ServerName serverName : serverNames) {
                String hostAndPort = serverName.getHostAndPort();
                String hostAndPortRoot = rootRegionLocation.getHostnamePort();
                if (hostAndPort.equals(hostAndPortRoot)) {
                    regions.put(rootRegionLocation.getRegionInfo(), serverName);
                    break;
                }
            }
        }

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    @SuppressWarnings("UnusedParameters")
    public static NavigableMap<HRegionInfo, ServerName> regionServerMap(Args args, Configuration conf, HConnection connection, final Set<String> tableNames, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<HRegionInfo, ServerName> regions = new TreeMap<>();
        if (tableNames.size() == 1) {
            return regionServerMap(args, conf, connection, tableNames.toArray(new String[1])[0], offlined);
        } else if (tableNames.size() > 1) {
            MetaScanner.BlockingMetaScannerVisitor visitor = new MetaScanner.BlockingMetaScannerVisitor(conf) {
                @Override
                public boolean processRowInternal(Result rowResult) throws IOException {
                    HRegionInfo info = Writables.getHRegionInfo(rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
                    byte[] value = rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
                    String hostAndPort = null;
                    if (value != null && value.length > 0) {
                        hostAndPort = Bytes.toString(value);
                    }
                    value = rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
                    long startcode = -1L;
                    if (value != null && value.length > 0) startcode = Bytes.toLong(value);
                    if (!(info.isOffline() || info.isSplit())) {
                        ServerName sn = null;
                        if (hostAndPort != null && hostAndPort.length() > 0) {
                            sn = new ServerName(hostAndPort, startcode);
                        }
                        if (info.isOffline() && !offlined) return true;

                        String tableName = info.getTableNameAsString();
                        if (tableNames.contains(tableName))
                            regions.put(info, sn);
                    }
                    return true;
                }
            };
            MetaScanner.metaScan(conf, visitor);
        }

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    @SuppressWarnings("UnusedParameters")
    private static NavigableMap<HRegionInfo, ServerName> regionServerMap(Args args, Configuration conf, HConnection connection, final String tableName, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<HRegionInfo, ServerName> regions = new TreeMap<>();
        MetaScanner.MetaScannerVisitor visitor = new MetaScanner.TableMetaScannerVisitor(conf, tableName.getBytes()) {
            @Override
            public boolean processRowInternal(Result rowResult) throws IOException {
                HRegionInfo info = Writables.getHRegionInfo(rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
                byte[] value = rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
                String hostAndPort = null;
                if (value != null && value.length > 0) {
                    hostAndPort = Bytes.toString(value);
                }
                value = rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
                long startcode = -1L;
                if (value != null && value.length > 0) startcode = Bytes.toLong(value);
                if (!(info.isOffline() || info.isSplit())) {
                    ServerName sn = null;
                    if (hostAndPort != null && hostAndPort.length() > 0) {
                        sn = new ServerName(hostAndPort, startcode);
                    }
                    if (info.isOffline() && !offlined) return true;
                    regions.put(info, sn);
                }
                return true;
            }
        };
        MetaScanner.metaScan(conf, visitor, tableName.getBytes());

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    public static String getTableName(HRegionInfo hRegionInfo) {
        return hRegionInfo.getTableNameAsString();
    }

    public static boolean mergeRegions(Args args, HBaseAdmin admin, HRegionInfo regionA, HRegionInfo regionB) throws IOException {
        throw new IllegalStateException("Not supported in this HBase version.");
    }

    public static List<HRegionInfo> adjacentEmptyRegions(List<HRegionInfo> emptyRegions) {
        throw new IllegalStateException("Not supported in this HBase version.");
    }

    public static boolean isMajorCompacting(Args args, HBaseAdmin admin, String tableName)
        throws IOException, InterruptedException {
        CompactionState compactionState = admin.getCompactionState(tableName);
        return !(compactionState == CompactionState.NONE || compactionState == CompactionState.MINOR);
    }
}