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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.*;

/**
 * For HBase 2.1
 */
public class CommandAdapter {
    public static List<RegionPlan> makePlan(Admin admin,
        Map<ServerName, List<RegionInfo>> clusterState, Configuration conf) throws IOException {
        StochasticLoadBalancer balancer = new StochasticLoadBalancer() {
            @Override
            protected boolean needsBalance(Cluster c) {
                return true;
            }
        };
        balancer.setConf(conf);
        balancer.setClusterMetrics(admin.getClusterMetrics());
        List<RegionPlan> regionPlanList = balancer.balanceCluster(clusterState);
        return regionPlanList == null ? new ArrayList<>() : regionPlanList;
    }

    public static List<RegionPlan> makePlan(Admin admin, List<RegionPlan> newRegionPlan) throws IOException {
        // snapshot current region assignment
        Map<RegionInfo, ServerName> regionAssignmentMap = createRegionAssignmentMap(admin);

        // update with new plan
        for (RegionPlan regionPlan : newRegionPlan) {
            regionAssignmentMap.put(regionPlan.getRegionInfo(), regionPlan.getDestination());
        }

        Map<ServerName, List<RegionInfo>> clusterState = initializeRegionMap(admin);
        for (Map.Entry<RegionInfo, ServerName> entry : regionAssignmentMap.entrySet())
            clusterState.get(entry.getValue()).add(entry.getKey());

        StochasticLoadBalancer balancer = new StochasticLoadBalancer();
        Configuration conf = admin.getConfiguration();
        conf.setFloat("hbase.regions.slop", 0.2f);
        balancer.setConf(conf);
        return balancer.balanceCluster(clusterState);
    }

    // contains catalog tables
    private static Map<RegionInfo, ServerName> createRegionAssignmentMap(Admin admin) throws IOException {
        Map<RegionInfo, ServerName> regionMap = new HashMap<>();
        for (ServerName serverName : admin.getRegionServers()) {
            for (RegionInfo hRegionInfo : admin.getRegions(serverName)) {
                regionMap.put(hRegionInfo, serverName);
            }
        }
        return regionMap;
    }

    public static List<RegionInfo> getOnlineRegions(Args args, Admin admin, ServerName serverName)
        throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, "getOnlineRegions - start");
        List<RegionInfo> onlineRegions = admin.getRegions(serverName);
        Util.printVerboseMessage(args, "getOnlineRegions - end", startTimestamp);
        return onlineRegions;
    }

    public static boolean isMetaTable(TableName tableName) {
        return tableName.equals(TableName.META_TABLE_NAME);
    }

    private static TableName metaTableName() {
        return TableName.META_TABLE_NAME;
    }

    public static boolean isBalancerRunning(Admin admin) throws IOException {
        boolean balancerRunning = admin.balancerSwitch(false, false);
        if (balancerRunning) {
            admin.balancerSwitch(true, false);
        }
        return balancerRunning;
    }

    public static Map<ServerName, List<RegionInfo>> initializeRegionMap(Admin admin) throws IOException {
        Map<ServerName, List<RegionInfo>> regionMap = new TreeMap<>();
        for (ServerName serverName : admin.getRegionServers())
            regionMap.put(serverName, new ArrayList<>());
        return regionMap;
    }

    public static void loginUserFromSubject(Configuration conf, Subject subject) throws IOException {
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(subject);
    }

    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    public static NavigableMap<RegionInfo, ServerName> regionServerMap(Args args, Admin admin, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<RegionInfo, ServerName> regions = new TreeMap<>();
        MetaTableAccessor.Visitor visitor = new MetaTableAccessor.DefaultVisitorBase() {
            @Override
            public boolean visitInternal(Result result) {
                RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
                if (locations == null) return true;
                for (HRegionLocation loc : locations.getRegionLocations()) {
                    if (loc != null) {
                        RegionInfo regionInfo = loc.getRegion();
                        if (regionInfo.getTable().getNameAsString().startsWith("hbase:")) return true;
                        if (regionInfo.isOffline() && !offlined) return true;
                        regions.put(regionInfo, loc.getServerName());
                    }
                }
                return true;
            }
        };
        MetaTableAccessor.fullScanRegions(admin.getConnection(), visitor);

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    public static NavigableMap<RegionInfo, ServerName> regionServerMap(Args args,
                                                                       Admin admin, final Set<TableName> tableNames, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<RegionInfo, ServerName> regions = new TreeMap<>();
        if (tableNames.size() == 1) {
            return regionServerMap(args, admin, tableNames.toArray(new TableName[1])[0], offlined);
        } else if (tableNames.size() > 1) {
            MetaTableAccessor.Visitor visitor = new MetaTableAccessor.DefaultVisitorBase() {
                @Override
                public boolean visitInternal(Result result) {
                    RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
                    if (locations == null) return true;
                    for (HRegionLocation loc : locations.getRegionLocations()) {
                        if (loc != null) {
                            RegionInfo regionInfo = loc.getRegion();
                            TableName table = regionInfo.getTable();
                            if (table.getNameAsString().startsWith("hbase:")) return true;
                            if (regionInfo.isOffline() && !offlined) return true;
                            if (tableNames.contains(table))
                                regions.put(regionInfo, loc.getServerName());
                        }
                    }
                    return true;
                }
            };
            MetaTableAccessor.fullScanRegions(admin.getConnection(), visitor);
        }

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    private static NavigableMap<RegionInfo, ServerName> regionServerMap(Args args,
                                                                        Admin admin, final TableName tableName, final boolean offlined) throws IOException {
        long timestamp = System.currentTimeMillis();

        final NavigableMap<RegionInfo, ServerName> regions = new TreeMap<>();
        MetaTableAccessor.Visitor visitor = new MetaTableAccessor.TableVisitorBase(tableName) {
            @Override
            public boolean visitInternal(Result result) {
                RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
                if (locations == null) return true;
                for (HRegionLocation loc : locations.getRegionLocations()) {
                    if (loc != null) {
                        RegionInfo regionInfo = loc.getRegion();
                        if (regionInfo.isOffline() && !offlined) return true;
                        regions.put(regionInfo, loc.getServerName());
                    }
                }
                return true;
            }
        };
        MetaTableAccessor.fullScanRegions(admin.getConnection(), visitor);

        Util.printVerboseMessage(args, "CommandAdapter.regionServerMap", timestamp);
        return regions;
    }

    public static TableName getTableName(RegionInfo hRegionInfo) {
        return hRegionInfo.getTable();
    }

    public static boolean mergeRegions(Args args, Admin admin,
        RegionInfo regionA, RegionInfo regionB) throws IOException {
        long timestamp = System.currentTimeMillis();

        if (RegionInfo.areAdjacent(regionA, regionB)) {
            admin.mergeRegionsAsync(regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
            Util.printVerboseMessage(args, "CommandAdapter.mergeRegions", timestamp);
            return true;
        } else {
            Util.printVerboseMessage(args, "CommandAdapter.mergeRegions", timestamp);
            return false;
        }
    }

    public static List<RegionInfo> adjacentEmptyRegions(List<RegionInfo> emptyRegions) {
        List<RegionInfo> adjacentEmptyRegions = new ArrayList<>();
        for (int i = 0; i < emptyRegions.size() - 1; i++) {
            RegionInfo regionA = emptyRegions.get(i);
            RegionInfo regionB = emptyRegions.get(i + 1);
            if (RegionInfo.areAdjacent(regionA, regionB)) {
                adjacentEmptyRegions.add(regionA);
                adjacentEmptyRegions.add(regionB);
                i++;
            }
        }
        return adjacentEmptyRegions;
    }

    @SuppressWarnings("UnusedParameters")
    public static boolean isMajorCompacting(Args args, Admin admin, TableName tableName)
        throws IOException {
        org.apache.hadoop.hbase.client.CompactionState compactionState = admin.getCompactionState(tableName);
        return compactionState == CompactionState.MAJOR_AND_MINOR || compactionState == CompactionState.MAJOR;
    }

    public static boolean isReallyEmptyRegion(Connection connection,
        String tableName, RegionInfo regionInfo) throws IOException {
        boolean emptyRegion = false;
        // verify really empty region by scanning records
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan().withStartRow(regionInfo.getStartKey()).withStopRow(regionInfo.getEndKey());
            FilterList filterList = new FilterList();
            filterList.addFilter(new KeyOnlyFilter());
            filterList.addFilter(new FirstKeyOnlyFilter());
            scan.setFilter(filterList);
            scan.setCacheBlocks(false);
            scan.setReadType(Scan.ReadType.PREAD);
            scan.setCaching(1);

            try (ResultScanner scanner = table.getScanner(scan)) {
                if (scanner.next() == null) emptyRegion = true;
            }
        }
        return emptyRegion;
    }

    @SuppressWarnings("deprecation")
    // tableName, encodeRegionName, regionServer
    public static Map<TableName, Map<String, String>> versionedRegionMap(Connection connection, long timestamp)
        throws IOException {
        Map<TableName, Map<String, String>> regionLocationMap = new HashMap<>();
        try (Table metaTable = connection.getTable(metaTableName())) {
            Scan scan = new Scan();
            scan.setSmall(true);
            scan.setCaching(1000);
            scan.setMaxVersions();
            ResultScanner scanner = metaTable.getScanner(scan);
            for (Result result : scanner) {
                RegionInfo regionInfo = MetaTableAccessor.getRegionInfo(result);
                List<Cell> columnCells = result.getColumnCells("info".getBytes(), "server".getBytes());
                for (Cell cell : columnCells) {
                    if (cell.getTimestamp() <= timestamp) {
                        TableName tableName = regionInfo.getTable();
                        String encodeRegionName = regionInfo.getEncodedName();
                        ServerName serverName = getServerName(result, cell);
                        if (serverName != null) {
                            String regionServerStr = serverName.toShortString().replaceAll(":", ",");

                            Map<String, String> innerMap = regionLocationMap.computeIfAbsent(tableName, k -> new HashMap<>());
                            innerMap.putIfAbsent(encodeRegionName, regionServerStr);
                        }
                    }
                }
            }
        }
        return regionLocationMap;
    }

    private static ServerName getServerName(Result result, Cell currentCell) {
        List<Cell> resultCells = new ArrayList<>();

        resultCells.add(currentCell);
        long timestamp = currentCell.getTimestamp();

        List<Cell> cells = result.getColumnCells("info".getBytes(), MetaTableAccessor.getStartCodeColumn(0));
        for (Cell cell : cells) {
            if (cell.getTimestamp() == timestamp) {
                resultCells.add(cell);
                return MetaTableAccessor.getServerName(Result.create(resultCells), 0);
            }
        }

        return null;
    }

    public static ServerName create(String serverNameStr) {
        return ServerName.valueOf(serverNameStr);
    }
}
