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

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import com.kakao.hbase.specific.RegionLoadDelegator;
import com.kakao.hbase.stat.load.TableInfo;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Merge implements Command {
    private final HBaseAdmin admin;
    private final Args args;
    private final String actionParam;
    private final Set<String> tableNameSet;
    private boolean proceed = false;
    private boolean test = false;
    private final HConnection connection;

    public Merge(HBaseAdmin admin, Args args) throws IOException {
        if (args.getOptionSet().nonOptionArguments().size() < 2
            || args.getOptionSet().nonOptionArguments().size() > 3) { // todo refactoring
            throw new RuntimeException(Args.INVALID_ARGUMENTS);
        }

        this.admin = admin;
        this.args = args;
        actionParam = (String) args.getOptionSet().nonOptionArguments().get(2);
        this.connection = HConnectionManager.createConnection(admin.getConfiguration());

        tableNameSet = Util.parseTableSet(admin, args);
    }

    @SuppressWarnings("unused")
    public static String usage() {
        return "Merge regions. It may take a long time.\n"
            + "usage: "
            + Merge.class.getSimpleName().toLowerCase()
            + " [options] <zookeeper quorum> <table name(regex)> <action>\n"
            + "  actions:\n"
            + "    empty-fast       - Merge adjacent 2 empty regions only.\n"
            + "    empty            - Merge all empty regions.\n"
            + "  options:\n"
            + "    --" + Args.OPTION_MAX_ITERATION + " - Set max iteration.\n"
            + Args.commonUsage();
    }

    private int getMaxMaxIteration() {
        if (args.has(Args.OPTION_MAX_ITERATION)) {
            return (int) args.valueOf(Args.OPTION_MAX_ITERATION);
        } else
            return Constant.TRY_MAX;
    }

    public void setTest(boolean test) {
        this.test = test;
    }

    @Override
    public void run() throws Exception {
        long timestampPrev;
        // todo refactoring
        if (actionParam.toLowerCase().equals("empty-fast")) {
            for (String tableName : tableNameSet) {
                timestampPrev = System.currentTimeMillis();
                TableInfo tableInfo = new TableInfo(admin, tableName, args);
                timestampPrev = Util.printVerboseMessage(args, "Merge.run.new TableInfo", timestampPrev);
                emptyFast(tableInfo);
                Util.printVerboseMessage(args, "Merge.run.emptyFast", timestampPrev);
            }
        } else if (actionParam.toLowerCase().equals("empty")) {
            for (String tableName : tableNameSet) {
                TableInfo tableInfo = new TableInfo(admin, tableName, args);

                System.out.println("Table - " + tableName + " - empty-fast - Start");
                emptyFast(tableInfo);
                System.out.println("Table - " + tableName + " - empty-fast - End\n");

                System.out.println("Table - " + tableName + " - empty - Start");
                empty(tableInfo);
                System.out.println("Table - " + tableName + " - empty - End");
            }
        } else if (actionParam.toLowerCase().equals("size")) {
            throw new IllegalStateException("Not implemented yet"); //todo
        } else {
            throw new IllegalArgumentException("Invalid merge action - " + actionParam);
        }
    }

    private long getMergeWaitIntervalMs() {
        if (test)
            return Constant.WAIT_INTERVAL_MS;
        else
            return Constant.LARGE_WAIT_INTERVAL_MS;
    }

    private void empty(TableInfo tableInfo) throws Exception {
        for (int i = 1; i <= getMaxMaxIteration(); i++) {
            try {
                if (!args.isForceProceed()) {
                    if (!proceed)
                        proceed = Util.askProceed();
                    if (!proceed) {
                        return;
                    }
                }

                if (emptyInternal(tableInfo)) break;
                System.out.println("Iteration " + i + "/" + getMaxMaxIteration() + " - Wait for "
                    + getMergeWaitIntervalMs() / 1000 + " seconds\n");
                Thread.sleep(getMergeWaitIntervalMs());
            } catch (IllegalStateException e) {
                if (e.getMessage().contains(Constant.MESSAGE_NEED_REFRESH)) {
                    Thread.sleep(Constant.WAIT_INTERVAL_MS);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * @param tableInfo
     * @return true: stop iteration, false: continue iteration
     * @throws Exception
     */
    private boolean emptyInternal(TableInfo tableInfo) throws Exception {
        tableInfo.refresh();

        Set<HRegionInfo> mergedRegions = new HashSet<>();
        List<HRegionInfo> allTableRegions = new ArrayList<>(tableInfo.getRegionInfoSet());
        for (int i = 0; i < allTableRegions.size(); i++) {
            HRegionInfo region = allTableRegions.get(i);
            if (mergedRegions.contains(region)) continue;

            RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(region);
            if (regionLoad == null) throw new IllegalStateException(Constant.MESSAGE_NEED_REFRESH);

            HRegionInfo targetRegion = getTargetRegion(tableInfo, allTableRegions, i, mergedRegions);
            if (mergedRegions.contains(targetRegion)) continue;

            if (regionLoad.getStorefileSizeMB() == 0 && regionLoad.getMemStoreSizeMB() == 0) {
                if (CommandAdapter.isReallyEmptyRegion(connection, tableInfo.getTableName(), region)) {
                    try {
                        if (targetRegion != null) {
                            printMergeInfo(region, targetRegion);
                            mergedRegions.add(region);
                            mergedRegions.add(targetRegion);
                            CommandAdapter.mergeRegions(args, admin, region, targetRegion);
                            i++;
                        }
                    } catch (RegionException e) {
                        throw new IllegalStateException(Constant.MESSAGE_NEED_REFRESH);
                    }
                }
            }
        }

        System.out.println();
        return mergedRegions.size() <= 1;
    }

    private HRegionInfo getTargetRegion(TableInfo tableInfo, List<HRegionInfo> allTableRegions,
        int i, Set<HRegionInfo> mergedRegions) {
        HRegionInfo regionPrev = i > 0 ? allTableRegions.get(i - 1) : null;
        if (mergedRegions.contains(regionPrev)) regionPrev = null;
        HRegionInfo regionNext = i == allTableRegions.size() - 1 ? null : allTableRegions.get(i + 1);

        int sizePrev = getSize(tableInfo, regionPrev);
        int sizeNext = getSize(tableInfo, regionNext);
        if (sizePrev <= sizeNext)
            return regionPrev;
        else
            return regionNext;
    }

    private int getSize(TableInfo tableInfo, HRegionInfo region) {
        if (region != null) {
            RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(region);
            if (regionLoad != null) {
                return regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB();
            }
        }

        return Integer.MAX_VALUE;
    }

    private void emptyFast(TableInfo tableInfo) throws Exception {
        long timestampPrev;
        boolean merged;
        for (int j = 1; j <= getMaxMaxIteration(); j++) {
            merged = false;

            timestampPrev = System.currentTimeMillis();
            List<HRegionInfo> emptyRegions = findEmptyRegions(tableInfo);
            timestampPrev = Util.printVerboseMessage(args, "Merge.emptyFast.findEmptyRegions", timestampPrev);
            if (emptyRegions.size() > 1) {
                List<HRegionInfo> adjacentEmptyRegions = CommandAdapter.adjacentEmptyRegions(emptyRegions);
                Util.printVerboseMessage(args, "Merge.emptyFast.adjacentEmptyRegions", timestampPrev);
                System.out.println();
                System.out.println("Iteration " + j + "/" + getMaxMaxIteration() + " - "
                    + adjacentEmptyRegions.size() + " adjacent empty regions are found");
                if (adjacentEmptyRegions.size() == 0) return;
                emptyRegions = adjacentEmptyRegions;
                if (!args.isForceProceed()) {
                    if (!proceed)
                        proceed = Util.askProceed();
                    if (!proceed) {
                        return;
                    }
                }
            } else {
                break;
            }

            for (int i = 0; i < emptyRegions.size(); i++) {
                HRegionInfo regionA = emptyRegions.get(i);
                if (i != emptyRegions.size() - 1) {
                    HRegionInfo regionB = emptyRegions.get(i + 1);

                    if (CommandAdapter.mergeRegions(args, admin, regionA, regionB)) {
                        i++;
                        printMergeInfo(regionA, regionB);
                        merged = true;
                    }
                }
                if (!merged)
                    System.out.println("Skip merging - " + regionA.getEncodedName()
                        + " - There is no adjacent empty region");
            }

            if (merged)
                Thread.sleep(Constant.WAIT_INTERVAL_MS);
            else
                break;
        }
    }

    private void printMergeInfo(HRegionInfo regionA, HRegionInfo regionB) {
        System.out.println("Merge regions - " + Util.getRegionInfoString(regionA));
        System.out.println("              L " + Util.getRegionInfoString(regionB));
    }

    private List<HRegionInfo> findEmptyRegions(TableInfo tableInfo) throws Exception {
        for (int i = 0; i < Constant.TRY_MAX; i++) {
            try {
                return findEmptyRegionsInternal(tableInfo);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains(Constant.MESSAGE_NEED_REFRESH)) {
                    Thread.sleep(Constant.WAIT_INTERVAL_MS);
                } else {
                    throw e;
                }
            }
        }

        throw new IllegalStateException("findEmptyRegions failed");
    }

    private List<HRegionInfo> findEmptyRegionsInternal(TableInfo tableInfo) throws Exception {
        List<HRegionInfo> emptyRegions = new ArrayList<>();

        tableInfo.refresh();
        Set<HRegionInfo> allTableRegions = tableInfo.getRegionInfoSet();
        for (HRegionInfo regionInfo : allTableRegions) {
            RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(regionInfo);
            if (regionLoad == null) throw new IllegalStateException(Constant.MESSAGE_NEED_REFRESH);

            if (regionLoad.getStorefileSizeMB() == 0 && regionLoad.getMemStoreSizeMB() == 0) {
                if (CommandAdapter.isReallyEmptyRegion(connection, tableInfo.getTableName(), regionInfo)) emptyRegions.add(regionInfo);
            }
        }

        return emptyRegions;
    }
}
