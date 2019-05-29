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
import com.kakao.hbase.specific.RegionLoadAdapter;
import com.kakao.hbase.specific.RegionLoadDelegator;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MC implements Command {
    private final Admin admin;
    private final Args args;
    private final AtomicInteger mcCounter = new AtomicInteger();
    private final Map<byte[], TableName> regionTableMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private final Map<byte[], Integer> regionSizeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private final Map<byte[], Float> regionLocalityMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private final Map<byte[], String> regionRSMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private Map<TableName, NavigableMap<RegionInfo, ServerName>> regionLocations = new HashMap<>();
    private Set<byte[]> targetRegions = null;
    private Set<TableName> targetTables = null;
    private boolean tableLevel = false;

    MC(Admin admin, Args args) {
        if (args.getOptionSet().nonOptionArguments().size() != 2) {
            throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
        }

        this.admin = admin;
        this.args = args;
    }

    @SuppressWarnings("unused")
    public static String usage() {
        return "Run major compaction on tables.\n"
            + "usage: " + MC.class.getSimpleName().toLowerCase() + " <zookeeper quorum> <table regex> [options]\n"
            + "  options:\n"
            + "    --" + Args.OPTION_WAIT_UNTIL_FINISH + ": Wait until all of MCs are finished.\n"
            + "    --" + Args.OPTION_INTERACTIVE + ": Ask whether to proceed MC for each region or table.\n"
            + "    --" + Args.OPTION_REGION_SERVER + "=<RS regex>: Compact the regions on these RSs.\n"
            + "    --" + Args.OPTION_CF + "=<CF>: Compact the regions of this CF.\n"
            + "    --" + Args.OPTION_LOCALITY_THRESHOLD
            + "=<threshold%>: Compact only if the data locality of the region is lower than this threshold.\n"
            + Args.commonUsage();
    }

    @VisibleForTesting
    int getMcCounter() {
        return mcCounter.get();
    }

    @VisibleForTesting
    Set<TableName> getTargetTables() {
        return targetTables;
    }

    @VisibleForTesting
    Set<byte[]> getTargetRegions() {
        return targetRegions;
    }

    @VisibleForTesting
    boolean isTableLevel() {
        return tableLevel;
    }

    @Override
    public void run() throws Exception {
        targetRegions = Collections.newSetFromMap(new TreeMap<>(Bytes.BYTES_COMPARATOR));
        targetTables = Collections.newSetFromMap(new TreeMap<>());
        tableLevel = false; // or region level

        Set<TableName> tables = Args.tables(args, admin);
        assert tables != null;
        for (TableName table : tables) {
            if (args.has(Args.OPTION_REGION_SERVER) || args.has(Args.OPTION_LOCALITY_THRESHOLD)) {
                // MC at region level
                tableLevel = false;

                if (args.has(Args.OPTION_REGION_SERVER)) {
                    filterWithRsAndLocality(targetRegions, table);
                } else {
                    if (args.has(Args.OPTION_LOCALITY_THRESHOLD)) {
                        filterWithLocalityOnly(targetRegions, table);
                    }
                }
            } else {
                // MC at table level
                tableLevel = true;

                targetTables.add(table);
            }
        }

        // todo check compaction queue before running

        if (tableLevel) {
            System.out.println(targetTables.size() + " tables will be compacted.");
        } else {
            System.out.println(targetRegions.size() + " regions will be compacted.");
        }
        if (targetTables.size() == 0 && targetRegions.size() == 0) return;
        if (!args.isForceProceed() && !Util.askProceed()) return;

        mc(tableLevel, targetRegions, targetTables);

        if (mcCounter.get() > 0)
            waitUntilFinish(tables);
    }

    private void mc(boolean tableLevel, Set<byte[]> targetRegions, Set<TableName> targetTables) throws IOException {
        int i = 1;
        if (tableLevel) {
            for (TableName tableName : targetTables) {
                if (args.has(Args.OPTION_CF)) {
                    String cf = (String) args.valueOf(Args.OPTION_CF);
                    try {
                        System.out.print(i++ + "/" + targetTables.size() + " - Major compaction on " + cf + " CF of " +
                                "table " + tableName.getNameAsString());
                        if (!Util.askProceedInteractively(args, true)) continue;
                        admin.majorCompact(tableName, cf.getBytes());
                        mcCounter.getAndIncrement();
                    } catch (IOException e) {
                        String message = "column family " + cf + " does not exist";
                        if (e.getMessage().contains(message)) {
                            System.out.println("WARNING - " + message + " on " + tableName.getNameAsString());
                        } else {
                            throw e;
                        }
                    }
                } else {
                    System.out.print(i++ + "/" + targetTables.size() + " - Major compaction on " + "table " + tableName.getNameAsString());
                    if (!Util.askProceedInteractively(args, true)) continue;
                    try {
                        admin.majorCompact(tableName);
                    } catch (NotServingRegionException ignore) {
                    }
                    mcCounter.getAndIncrement();
                }
            }
        } else {
            for (byte[] region : targetRegions) {
                if (args.has(Args.OPTION_CF)) {
                    String cf = (String) args.valueOf(Args.OPTION_CF);
                    try {
                        System.out.print(i++ + "/" + targetRegions.size() + " - Major compaction on " + cf + " CF of " +
                                "region " + Bytes.toStringBinary(region) + " - " + getRegionInfo(region));
                        if (!Util.askProceedInteractively(args, true)) continue;
                        admin.majorCompactRegion(region, cf.getBytes());
                        mcCounter.getAndIncrement();
                    } catch (IOException e) {
                        String message = "column family " + cf + " does not exist";
                        if (e.getMessage().contains(message)) {
                            System.out.println("WARNING - " + message + " on " + Bytes.toStringBinary(region));
                        } else {
                            throw e;
                        }
                    }
                } else {
                    System.out.print(i++ + "/" + targetRegions.size() + " - Major compaction on "
                            + "region " + Bytes.toStringBinary(region) + " - " + getRegionInfo(region));
                    if (!Util.askProceedInteractively(args, true)) continue;
                    try {
                        admin.majorCompactRegion(region);
                    } catch (NotServingRegionException ignore) {
                    }
                    mcCounter.getAndIncrement();
                }
            }
        }
    }

    private String getRegionInfo(byte[] regionName) {
        return "Table: " + regionTableMap.get(regionName)
            + ", RS: " + regionRSMap.get(regionName)
            + ", Locality: " + (regionLocalityMap.get(regionName) == null ? "null" :
            StringUtils.formatPercent(regionLocalityMap.get(regionName), 2))
            + ", SizeMB: " + regionSizeMap.get(regionName);
    }

    private void waitUntilFinish(Set<TableName> tables) throws IOException, InterruptedException {
        if (args.has(Args.OPTION_WAIT_UNTIL_FINISH)) {
            long sleepDuration = args.has(Args.OPTION_TEST) ?
                Constant.SMALL_WAIT_INTERVAL_MS : Constant.LARGE_WAIT_INTERVAL_MS;
            long timestamp = System.currentTimeMillis();

            System.out.print("Running ");

            while (true) {
                // sleep first
                for (int j = 0; j < 6; j++) {
                    System.out.print(".");
                    Thread.sleep(sleepDuration / 6);
                }

                int i = 0;
                for (TableName table : tables) {
                    if (!CommandAdapter.isMajorCompacting(args, admin, table)) {
                        i++;
                    }
                }
                if (i == tables.size()) break;
            }

            System.out.println();
            System.out.println("All of MCs are finished.");
            System.out.println("Duration: " + (System.currentTimeMillis() - timestamp) / 1000 + " secs");
        }
    }

    private void filterWithLocalityOnly(Set<byte[]> targetRegions, TableName table) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        Map<byte[], RegionInfo> regionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<RegionInfo, ServerName> entry : getRegionLocations(table).entrySet()) {
            byte[] regionName = entry.getKey().getRegionName();
            String serverName = entry.getValue().getHostname();
            regionMap.put(entry.getKey().getRegionName(), entry.getKey());
            regionTableMap.put(regionName, table);
            regionRSMap.put(regionName, serverName);
        }

        filterWithDataLocality(targetRegions, regionMap);

        Util.printVerboseMessage(args, Util.getMethodName() + " - end", startTimestamp);
    }

    private void filterWithRsAndLocality(Set<byte[]> targets, TableName table) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        Map<byte[], RegionInfo> regionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        String regex = (String) args.valueOf(Args.OPTION_REGION_SERVER);
        for (Map.Entry<RegionInfo, ServerName> entry : getRegionLocations(table).entrySet()) {
            String serverName = entry.getValue().getHostname() + "," + entry.getValue().getPort();
            if (serverName.matches(regex)) {
                regionMap.put(entry.getKey().getRegionName(), entry.getKey());
                byte[] regionName = entry.getKey().getRegionName();
                targets.add(regionName);
                regionTableMap.put(regionName, table);
                regionRSMap.put(regionName, serverName);
            }
        }

        filterWithDataLocality(targets, regionMap);

        Util.printVerboseMessage(args, Util.getMethodName() + " - end", startTimestamp);
    }

    private NavigableMap<RegionInfo, ServerName> getRegionLocations(TableName table) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        NavigableMap<RegionInfo, ServerName> result = regionLocations.get(table);
        if (result == null) {
            result = Util.getRegionLocationsMap(admin.getConnection(), table);
            regionLocations.put(table, result);
        }

        Util.printVerboseMessage(args, Util.getMethodName() +  " - end", startTimestamp);

        return result;
    }

    private void filterWithDataLocality(Set<byte[]> targetRegions,
        Map<byte[], RegionInfo> regionMap) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        final Double dataLocalityThreshold;
        if (args.has(Args.OPTION_LOCALITY_THRESHOLD)) {
            dataLocalityThreshold = (Double) args.valueOf(Args.OPTION_LOCALITY_THRESHOLD);
            if (dataLocalityThreshold < 1 || dataLocalityThreshold > 100)
                throw new IllegalArgumentException("Invalid data locality");
        } else {
            dataLocalityThreshold = null;
        }

        RegionLoadAdapter regionLoadAdapter = new RegionLoadAdapter(admin, regionMap, args);
        for (RegionInfo regionInfo : regionMap.values()) {
            RegionLoadDelegator regionLoad = regionLoadAdapter.get(regionInfo);
            if (regionLoad == null) continue;
            try {
                byte[] regionName = regionInfo.getRegionName();
                regionSizeMap.put(regionName, regionLoad.getStorefileSizeMB());
                if (dataLocalityThreshold == null) {
                    targetRegions.add(regionName);
                } else {
                    float dataLocality = regionLoad.getDataLocality();
                    regionLocalityMap.put(regionName, dataLocality);
                    if (dataLocality * 100 < dataLocalityThreshold) targetRegions.add(regionName);
                }
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("not implemented")) {
                    throw new IllegalStateException("Option " + Args.OPTION_LOCALITY_THRESHOLD
                        + " is not supported in this HBase version.");
                } else {
                    throw e;
                }
            }
        }

        Util.printVerboseMessage(args, Util.getMethodName() + " - end", startTimestamp);
    }
}
