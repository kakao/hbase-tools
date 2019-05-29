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
import com.kakao.hbase.common.EmptyRegionChecker;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import com.kakao.hbase.specific.RegionLoadDelegator;
import com.kakao.hbase.stat.load.TableInfo;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Merge implements Command {
    private final Admin admin;
    private final Args args;
    private final String actionParam;
    private final Set<TableName> tableNameSet;
    private boolean proceed = false;
    private boolean test = false;
    private boolean isPhoenixSaltingTable;

    Merge(Admin admin, Args args) throws IOException {
        if (args.getOptionSet().nonOptionArguments().size() < 2
                || args.getOptionSet().nonOptionArguments().size() > 3) { // todo refactoring
            throw new RuntimeException(Args.INVALID_ARGUMENTS);
        }

        this.admin = admin;
        this.args = args;
        if (args.has(Args.OPTION_TEST)) this.test = true;
        actionParam = (String) args.getOptionSet().nonOptionArguments().get(2);

        tableNameSet = Util.parseTableSet(admin, args);

        if (args.has(Args.OPTION_PHOENIX)) this.isPhoenixSaltingTable = true;
    }

    @SuppressWarnings("unused")
    public static String usage() {
        return "Merge regions. It may take a long time.\n"
                + "usage: "
                + Merge.class.getSimpleName().toLowerCase()
                + " <zookeeper quorum> <table name(regex)> <action> [options]\n"
                + "  actions:\n"
                + "    empty-fast       - Merge adjacent 2 empty regions only.\n"
                + "    empty            - Merge all empty regions.\n"
                + "  options:\n"
                + "    --" + Args.OPTION_MAX_ITERATION + " - Set max iteration.\n"
                + "    --" + Args.OPTION_PHOENIX + " - Set if the table to be merged is phoenix salted table.\n"
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
            for (TableName tableName : tableNameSet) {
                timestampPrev = System.currentTimeMillis();
                TableInfo tableInfo = new TableInfo(admin, tableName.getNameAsString(), args);
                timestampPrev = Util.printVerboseMessage(args, "Merge.run.new TableInfo", timestampPrev);
                emptyFast(tableInfo);
                Util.printVerboseMessage(args, "Merge.run.emptyFast", timestampPrev);
            }
        } else if (actionParam.toLowerCase().equals("empty")) {
            for (TableName tableName : tableNameSet) {
                TableInfo tableInfo = new TableInfo(admin, tableName.getNameAsString(), args);

                Util.printMessage("Table - " + tableName + " - empty-fast - Start");
                emptyFast(tableInfo);
                Util.printMessage("Table - " + tableName + " - empty-fast - End\n");

                Util.printMessage("Table - " + tableName + " - empty - Start");
                empty(tableInfo);
                Util.printMessage("Table - " + tableName + " - empty - End");
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
                Util.printMessage("Iteration " + i + "/" + getMaxMaxIteration() + " - Wait for "
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
     * @return true: stop iteration, false: continue iteration
     */
    private boolean emptyInternal(TableInfo tableInfo) throws Exception {
        tableInfo.refresh();

        Set<RegionInfo> mergedRegions = new HashSet<>();
        List<RegionInfo> allTableRegions = new ArrayList<>(tableInfo.getRegionInfoSet());
        for (int i = 0; i < allTableRegions.size(); i++) {
            RegionInfo region = allTableRegions.get(i);
            if (mergedRegions.contains(region)) continue;

            RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(region);
            if (regionLoad == null) throw new IllegalStateException(Constant.MESSAGE_NEED_REFRESH);

            RegionInfo targetRegion = getTargetRegion(tableInfo, allTableRegions, i, mergedRegions);
            if (mergedRegions.contains(targetRegion)) continue;

            if (regionLoad.getStorefileSizeMB() == 0 && regionLoad.getMemStoreSizeMB() == 0) {
                if (CommandAdapter.isReallyEmptyRegion(admin.getConnection(), tableInfo.getTableNamePattern(), region)) {
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

    private RegionInfo getTargetRegion(TableInfo tableInfo, List<RegionInfo> allTableRegions,
                                        int i, Set<RegionInfo> mergedRegions) {
        RegionInfo regionPrev = i > 0 ? allTableRegions.get(i - 1) : null;
        if (mergedRegions.contains(regionPrev)) regionPrev = null;
        RegionInfo regionNext = i == allTableRegions.size() - 1 ? null : allTableRegions.get(i + 1);

        int sizePrev = getSize(tableInfo, regionPrev);
        int sizeNext = getSize(tableInfo, regionNext);
        if (sizePrev <= sizeNext)
            return regionPrev;
        else
            return regionNext;
    }

    private int getSize(TableInfo tableInfo, RegionInfo region) {
        if (region != null) {
            RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(region);
            if (regionLoad != null) {
                return regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB();
            }
        }

        return Integer.MAX_VALUE;
    }

    private boolean isRegionBoundaryOfPhoenixSaltingTable(RegionInfo regionInfo) {
        byte[] endKey = regionInfo.getEndKey();
        boolean boundaryRegionForPhoenix = true;

        if (endKey.length > 0) {
            for (int i = 1, limit = endKey.length; i < limit; i++) {
                if (endKey[i] != 0) {
                    boundaryRegionForPhoenix = false;
                    break;
                }
            }
        }

        if (boundaryRegionForPhoenix) {
            Util.printVerboseMessage(args, regionInfo.getEncodedName() + " is boundary region of phoenix : "
                    + Bytes.toStringBinary(regionInfo.getStartKey()) + " ~ " + Bytes.toStringBinary(regionInfo.getEndKey()));
        }

        return boundaryRegionForPhoenix;
    }

    private void emptyFast(TableInfo tableInfo) throws Exception {
        long timestampPrev;
        boolean merged;
        for (int j = 1; j <= getMaxMaxIteration(); j++) {
            merged = false;

            timestampPrev = System.currentTimeMillis();
            List<RegionInfo> emptyRegions = findEmptyRegions(tableInfo);
            timestampPrev = Util.printVerboseMessage(args, "Merge.emptyFast.findEmptyRegions", timestampPrev);
            if (emptyRegions.size() > 1) {
                List<RegionInfo> adjacentEmptyRegions = CommandAdapter.adjacentEmptyRegions(emptyRegions);
                Util.printVerboseMessage(args, "Merge.emptyFast.adjacentEmptyRegions", timestampPrev);
                System.out.println();
                Util.printMessage("Iteration " + j + "/" + getMaxMaxIteration() + " - "
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
                RegionInfo regionA = emptyRegions.get(i);
                // 첫번째 리전의 endKey가 피닉스 솔팅 테이블의 리전 바운더리가 아니여야 한다.
                if (isPhoenixSaltingTable && isRegionBoundaryOfPhoenixSaltingTable(regionA)) {
                    continue;
                }

                if (i != emptyRegions.size() - 1) {
                    RegionInfo regionB = emptyRegions.get(i + 1);

                    boolean mergeRegions = false;
                    for (int k = 0; k < Constant.TRY_MAX_SMALL; k++) {
                        try {
                            mergeRegions = CommandAdapter.mergeRegions(args, admin, regionA, regionB);
                            break;
                        } catch (Exception e) {
                            e.printStackTrace();
                            Thread.sleep(Constant.WAIT_INTERVAL_MS);
                        }
                    }
                    if (mergeRegions) {
                        i++;
                        printMergeInfo(regionA, regionB);
                        merged = true;
                    }
                }
                if (!merged)
                    Util.printMessage("Skip merging - " + regionA.getEncodedName()
                            + " - There is no adjacent empty region");
            }

            if (merged) {
                System.out.println("Sleeping for "
                        + (getMergeWaitIntervalMs() / 1000) + " seconds.");
                Thread.sleep(getMergeWaitIntervalMs());
            } else {
                break;
            }
        }
    }

    private void printMergeInfo(RegionInfo regionA, RegionInfo regionB) {
        Util.printMessage("Merge regions");
        System.out.println("  - " + Util.getRegionInfoString(regionA));
        System.out.println("  L " + Util.getRegionInfoString(regionB));
    }

    private List<RegionInfo> findEmptyRegions(TableInfo tableInfo) throws Exception {
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

    private List<RegionInfo> findEmptyRegionsInternal(TableInfo tableInfo) throws Exception {
        long timestamp = System.currentTimeMillis();

        Set<RegionInfo> emptyRegions =
                Collections.synchronizedSet(Collections.newSetFromMap(new TreeMap<RegionInfo, Boolean>()));

        tableInfo.refresh();

        ExecutorService executorService = Executors.newFixedThreadPool(EmptyRegionChecker.THREAD_POOL_SIZE);
        try {
            EmptyRegionChecker.resetCounter();

            Set<RegionInfo> allTableRegions = tableInfo.getRegionInfoSet();
            for (RegionInfo regionInfo : allTableRegions) {
                RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(regionInfo);
                if (regionLoad == null) {
                    Util.printMessage("RegionLoad is empty - " + regionInfo);
                    throw new IllegalStateException(Constant.MESSAGE_NEED_REFRESH);
                }

                if (regionLoad.getStorefileSizeMB() == 0 && regionLoad.getMemStoreSizeMB() == 0) {
                    executorService.execute(
                            new EmptyRegionChecker(admin.getConnection(), tableInfo.getTableNamePattern(), regionInfo, emptyRegions));
                }
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.MINUTES);
        }
        Util.printMessage(emptyRegions.size() + " empty regions are found.");

        Util.printVerboseMessage(args, Util.getMethodName(), timestamp);
        return new ArrayList<>(emptyRegions);
    }
}
