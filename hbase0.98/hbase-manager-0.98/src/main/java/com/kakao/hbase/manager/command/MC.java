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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MC implements Command {
    private final HBaseAdmin admin;
    private final Args args;
    private final AtomicInteger mcCounter = new AtomicInteger();
    private final Map<byte[], String> regionTableMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private final Map<byte[], Integer> regionSizeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private final Map<byte[], Float> regionLocalityMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private final Map<byte[], String> regionRSMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private Map<String, NavigableMap<HRegionInfo, ServerName>> regionLocations = new HashMap<>();
    // regions or tables
    private Set<byte[]> targets = null;
    private boolean tableLevel = false;

    public MC(HBaseAdmin admin, Args args) {
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
    Set<byte[]> getTargets() {
        return targets;
    }

    @VisibleForTesting
    boolean isTableLevel() {
        return tableLevel;
    }

    @Override
    public void run() throws Exception {
        targets = Collections.newSetFromMap(new TreeMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR));
        tableLevel = false; // or region level

        Set<String> tables = Args.tables(args, admin);
        assert tables != null;
        for (String table : tables) {
            if (args.has(Args.OPTION_REGION_SERVER) || args.has(Args.OPTION_LOCALITY_THRESHOLD)) {
                // MC at region level
                tableLevel = false;

                if (args.has(Args.OPTION_REGION_SERVER)) {
                    filterWithRsAndLocality(targets, table);
                } else {
                    if (args.has(Args.OPTION_LOCALITY_THRESHOLD)) {
                        filterWithLocalityOnly(targets, table);
                    }
                }
            } else {
                // MC at table level
                tableLevel = true;

                targets.add(table.getBytes());
            }
        }

        // todo check compaction queue before running

        if (tableLevel) {
            System.out.println(targets.size() + " tables will be compacted.");
        } else {
            System.out.println(targets.size() + " regions will be compacted.");
        }
        if (targets.size() == 0) return;
        if (!args.isForceProceed() && !Util.askProceed()) return;

        mc(tableLevel, targets);

        if (mcCounter.get() > 0)
            waitUntilFinish(tables);
    }

    private void mc(boolean tableLevel, Set<byte[]> targets) throws InterruptedException, IOException {
        int i = 1;
        for (byte[] tableOrRegion : targets) {
            if (args.has(Args.OPTION_CF)) {
                String cf = (String) args.valueOf(Args.OPTION_CF);
                try {
                    System.out.print(i++ + "/" + targets.size() + " - Major compaction on " + cf + " CF of " +
                        (tableLevel ? "table " : "region ") + Bytes.toStringBinary(tableOrRegion) +
                        (tableLevel ? "" : " - " + getRegionInfo(tableOrRegion)));
                    if (!askProceedInteractively()) continue;
                    admin.majorCompact(tableOrRegion, cf.getBytes());
                    mcCounter.getAndIncrement();
                } catch (IOException e) {
                    String message = "column family " + cf + " does not exist";
                    if (e.getMessage().contains(message)) {
                        System.out.println("WARNING - " + message + " on " + Bytes.toStringBinary(tableOrRegion));
                    } else {
                        throw e;
                    }
                }
            } else {
                System.out.print(i++ + "/" + targets.size() + " - Major compaction on "
                    + (tableLevel ? "table " : "region ")
                    + Bytes.toStringBinary(tableOrRegion) + (tableLevel ? "" : " - " + getRegionInfo(tableOrRegion)));
                if (!askProceedInteractively()) continue;
                admin.majorCompact(tableOrRegion);
                mcCounter.getAndIncrement();
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

    private boolean askProceedInteractively() {
        if (args.has(Args.OPTION_INTERACTIVE)) {
            if (args.has(Args.OPTION_FORCE_PROCEED)) {
                System.out.println();
            } else {
                System.out.print(" - ");
                if (!Util.askProceed()) return false;
            }
        } else {
            System.out.println();
        }
        return true;
    }

    private void waitUntilFinish(Set<String> tables) throws IOException, InterruptedException {
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
                for (String table : tables) {
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

    private void filterWithLocalityOnly(Set<byte[]> targets, String table) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        Map<byte[], HRegionInfo> regionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<HRegionInfo, ServerName> entry : getRegionLocations(table).entrySet()) {
            byte[] regionName = entry.getKey().getRegionName();
            String serverName = entry.getValue().getHostname();
            regionMap.put(entry.getKey().getRegionName(), entry.getKey());
            regionTableMap.put(regionName, table);
            regionRSMap.put(regionName, serverName);
        }

        filterWithDataLocality(targets, regionMap);

        Util.printVerboseMessage(args, Util.getMethodName() + " - end", startTimestamp);
    }

    private void filterWithRsAndLocality(Set<byte[]> targets, String table) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        Map<byte[], HRegionInfo> regionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        String regex = (String) args.valueOf(Args.OPTION_REGION_SERVER);
        for (Map.Entry<HRegionInfo, ServerName> entry : getRegionLocations(table).entrySet()) {
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

    private NavigableMap<HRegionInfo, ServerName> getRegionLocations(String table) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");

        NavigableMap<HRegionInfo, ServerName> result = regionLocations.get(table);
        Configuration conf = admin.getConfiguration();
        conf.set("hbase.meta.scanner.caching", "1000");
        if (result == null) {
            try (HTable htable = new HTable(conf, table)) {
                result = htable.getRegionLocations();
                regionLocations.put(table, result);
            }
        }

        Util.printVerboseMessage(args, Util.getMethodName() +  " - end", startTimestamp);

        return result;
    }

    private void filterWithDataLocality(Set<byte[]> targets,
        Map<byte[], HRegionInfo> regionMap) throws IOException {
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
        for (HRegionInfo regionInfo : regionMap.values()) {
            RegionLoadDelegator regionLoad = regionLoadAdapter.get(regionInfo);
            if (regionLoad == null) continue;
            try {
                byte[] regionName = regionInfo.getRegionName();
                regionSizeMap.put(regionName, regionLoad.getStorefileSizeMB());
                if (dataLocalityThreshold == null) {
                    targets.add(regionName);
                } else {
                    float dataLocality = regionLoad.getDataLocality();
                    regionLocalityMap.put(regionName, dataLocality);
                    if (dataLocality * 100 < dataLocalityThreshold) targets.add(regionName);
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
