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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.ClusterLoadState;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.*;

/**
 * For HBase 0.98/0.96
 */
public class CommandAdapter {

    public static List<RegionPlan> makePlan(HBaseAdmin admin,
        Map<ServerName, List<HRegionInfo>> clusterState, Configuration conf) throws IOException {
        StochasticLoadBalancer balancer = new StochasticLoadBalancer() {
            @Override
            protected boolean needsBalance(ClusterLoadState cs) {
                return true;
            }
        };
        balancer.setConf(conf);
        balancer.setClusterStatus(admin.getClusterStatus());
        List<RegionPlan> regionPlanList = balancer.balanceCluster(clusterState);
        return regionPlanList == null ? new ArrayList<RegionPlan>() : regionPlanList;
    }

    public static List<RegionPlan> makePlan(HBaseAdmin admin, List<RegionPlan> newRegionPlan) throws IOException {
        // snapshot current region assignment
        Map<HRegionInfo, ServerName> regionAssignmentMap = createRegionAssignmentMap(admin);

        // update with new plan
        for (RegionPlan regionPlan : newRegionPlan) {
            regionAssignmentMap.put(regionPlan.getRegionInfo(), regionPlan.getDestination());
        }

        Map<ServerName, List<HRegionInfo>> clusterState = initializeRegionMap(admin);
        for (Map.Entry<HRegionInfo, ServerName> entry : regionAssignmentMap.entrySet())
            clusterState.get(entry.getValue()).add(entry.getKey());

        StochasticLoadBalancer balancer = new StochasticLoadBalancer();
        Configuration conf = admin.getConfiguration();
        conf.setFloat("hbase.regions.slop", 0.2f);
        balancer.setConf(conf);
        return balancer.balanceCluster(clusterState);
    }

    // contains catalog tables
    private static Map<HRegionInfo, ServerName> createRegionAssignmentMap(HBaseAdmin admin) throws IOException {
        Map<HRegionInfo, ServerName> regionMap = new HashMap<>();
        for (ServerName serverName : admin.getClusterStatus().getServers()) {
            for (HRegionInfo hRegionInfo : admin.getOnlineRegions(serverName)) {
                regionMap.put(hRegionInfo, serverName);
            }
        }
        return regionMap;
    }

    public static List<HRegionInfo> getOnlineRegions(Args args, HBaseAdmin admin, ServerName serverName)
        throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, "getOnlineRegions - start");
        List<HRegionInfo> onlineRegions = admin.getOnlineRegions(serverName);
        Util.printVerboseMessage(args, "getOnlineRegions - end", startTimestamp);
        return onlineRegions;
    }

    public static boolean isMetaTable(String tableName) {
        return tableName.equals(TableName.META_TABLE_NAME.getNameAsString());
    }

    public static String metaTableName() {
        return TableName.META_TABLE_NAME.getNameAsString();
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

    public static void loginUserFromSubject(Configuration conf, Subject subject) throws IOException {
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(subject);
    }

    @SuppressWarnings("UnusedParameters")
    public static NavigableMap<HRegionInfo, ServerName> regionServerMap(Args args, Configuration conf,
        HConnection connection, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<HRegionInfo, ServerName> regionServerMap = new TreeMap<>();
        MetaScanner.DefaultMetaScannerVisitor visitor = new MetaScanner.DefaultMetaScannerVisitor() {
            @Override
            public boolean processRowInternal(Result rowResult) throws IOException {
                HRegionInfo info = HRegionInfo.getHRegionInfo(rowResult);
                ServerName serverName = HRegionInfo.getServerName(rowResult);

                if (info.getTable().getNameAsString().startsWith("hbase:")) return true;
                if (info.isOffline() && !offlined) return true;
                regionServerMap.put(info, serverName);
                return true;
            }
        };
        MetaScanner.metaScan(conf, visitor);

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regionServerMap;
    }

    @SuppressWarnings("UnusedParameters")
    public static NavigableMap<HRegionInfo, ServerName> regionServerMap(Args args, Configuration conf,
        HConnection connection, final Set<String> tableNames, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<HRegionInfo, ServerName> regionServerMap = new TreeMap<>();

        if (tableNames.size() == 1) {
            return regionServerMap(args, conf, connection, tableNames.toArray(new String[1])[0], offlined);
        } else if (tableNames.size() > 1) {
            MetaScanner.DefaultMetaScannerVisitor visitor = new MetaScanner.DefaultMetaScannerVisitor() {
                @Override
                public boolean processRowInternal(Result rowResult) throws IOException {
                    HRegionInfo info = HRegionInfo.getHRegionInfo(rowResult);
                    ServerName serverName = HRegionInfo.getServerName(rowResult);

                    String tableName = info.getTable().getNameAsString();
                    if (tableName.startsWith("hbase:")) return true;
                    if (info.isOffline() && !offlined) return true;

                    if (tableNames.contains(tableName))
                        regionServerMap.put(info, serverName);
                    return true;
                }
            };
            MetaScanner.metaScan(conf, visitor);
        }

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regionServerMap;
    }

    private static NavigableMap<HRegionInfo, ServerName> regionServerMap(Args args, Configuration conf,
        HConnection connection, final String tableNameParam, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<HRegionInfo, ServerName> regions = new TreeMap<>();
        TableName tableName = TableName.valueOf(tableNameParam);
        MetaScanner.MetaScannerVisitor visitor = new MetaScanner.TableMetaScannerVisitor(tableName) {
            @Override
            public boolean processRowInternal(Result rowResult) throws IOException {
                HRegionInfo info = HRegionInfo.getHRegionInfo(rowResult);
                ServerName serverName = HRegionInfo.getServerName(rowResult);

                if (info.isOffline() && !offlined) return true;
                regions.put(info, serverName);
                return true;
            }
        };
        MetaScanner.metaScan(conf, connection, visitor, tableName);

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    public static String getTableName(HRegionInfo hRegionInfo) {
        return hRegionInfo.getTable().getNameAsString();
    }

    public static boolean mergeRegions(Args args, HBaseAdmin admin,
        HRegionInfo regionA, HRegionInfo regionB) throws IOException {
        long timestamp = System.currentTimeMillis();

        if (HRegionInfo.areAdjacent(regionA, regionB)) {
            admin.mergeRegions(regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
            Util.printVerboseMessage(args, "CommandAdapter.mergeRegions", timestamp);
            return true;
        } else {
            Util.printVerboseMessage(args, "CommandAdapter.mergeRegions", timestamp);
            return false;
        }
    }

    public static List<HRegionInfo> adjacentEmptyRegions(List<HRegionInfo> emptyRegions) {
        List<HRegionInfo> adjacentEmptyRegions = new ArrayList<>();
        for (int i = 0; i < emptyRegions.size() - 1; i++) {
            HRegionInfo regionA = emptyRegions.get(i);
            HRegionInfo regionB = emptyRegions.get(i + 1);
            if (HRegionInfo.areAdjacent(regionA, regionB)) {
                adjacentEmptyRegions.add(regionA);
                adjacentEmptyRegions.add(regionB);
                i++;
            }
        }
        return adjacentEmptyRegions;
    }

    @SuppressWarnings("UnusedParameters")
    public static boolean isMajorCompacting(Args args, HBaseAdmin admin, String tableName)
        throws IOException, InterruptedException {
        CompactionState compactionState = admin.getCompactionState(tableName);
        return compactionState == CompactionState.MAJOR_AND_MINOR || compactionState == CompactionState.MAJOR;
    }

    public static boolean isReallyEmptyRegion(HConnection connection,
        String tableName, HRegionInfo regionInfo) throws IOException {
        boolean emptyRegion = false;
        // verify really empty region by scanning records
        try (HTableInterface table = connection.getTable(tableName)) {
            Scan scan = new Scan(regionInfo.getStartKey(), regionInfo.getEndKey());
            FilterList filterList = new FilterList();
            filterList.addFilter(new KeyOnlyFilter());
            filterList.addFilter(new FirstKeyOnlyFilter());
            scan.setFilter(filterList);
            scan.setCacheBlocks(false);
            scan.setSmall(true);
            scan.setCaching(1);

            try (ResultScanner scanner = table.getScanner(scan)) {
                if (scanner.next() == null) emptyRegion = true;
            }
        }
        return emptyRegion;
    }

    @SuppressWarnings("deprecation")
    public static Map<String, Map<String, String>> versionedRegionMap(HBaseAdmin admin, long timestamp)
        throws IOException {
        Map<String, Map<String, String>> regionLocationMap = new HashMap<>();
        try (HTable metaTable = new HTable(admin.getConfiguration(), metaTableName())) {
            Scan scan = new Scan();
            scan.setSmall(true);
            scan.setCaching(1000);
            scan.setMaxVersions();
            ResultScanner scanner = metaTable.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> columnCells = result.getColumnCells("info".getBytes(), "server".getBytes());
                for (Cell cell : columnCells) {
                    if (cell.getTimestamp() <= timestamp) {
                        String tableName = Bytes.toString(HRegionInfo.getTableName(cell.getRow()));
                        String encodeRegionName = HRegionInfo.encodeRegionName(cell.getRow());
                        String regionServer = Bytes.toString(cell.getValue()).replaceAll(":", ",");

                        Map<String, String> innerMap = regionLocationMap.get(tableName);
                        if (innerMap == null) {
                            innerMap = new HashMap<>();
                            regionLocationMap.put(tableName, innerMap);
                        }
                        if (innerMap.get(encodeRegionName) == null)
                            innerMap.put(encodeRegionName, regionServer);
                    }
                }
            }
        }
        return regionLocationMap;
    }
}