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
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("unused")
enum AssignAction {
    EMPTY {
        @Override
        public void run(Admin admin, Args args) throws IOException, InterruptedException {
            final boolean balancerRunning = isBalancerRunning(admin, args);

            try {
                List<ServerName> targetList = Common.regionServers(admin);
                String sourceRsRegex = (String) args.getOptionSet().nonOptionArguments().get(2);
                final String expFileName = args.has(Args.OPTION_SKIP_EXPORT) ? null :
                    (String) args.getOptionSet().nonOptionArguments().get(3);
                if (args.getOptionSet().nonOptionArguments().size() > 4) {
                    throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                }

                empty(admin, args, targetList, sourceRsRegex, expFileName);
            } finally {
                if (balancerRunning) setBalancerRunning(admin, true);
            }
        }

        private void empty(Admin admin, Args args, List<ServerName> targetList,
            String sourceRsRegex, String expFileName) throws IOException, InterruptedException {
            List<ServerName> sourceServerNames = removeSource(sourceRsRegex, targetList);

            if (sourceServerNames.size() > 0) {
                printSourceRSs(sourceServerNames);
                if (!args.isForceProceed()) {
                    if (!Util.askProceed()) {
                        return;
                    }
                }

                if (expFileName != null) {
                    export(args, admin, expFileName, sourceRsRegex);
                }

                for (ServerName sourceServerName : sourceServerNames) {
                    List<Triple<TableName, String, String>> plan = plan(args, admin, targetList, sourceServerName);

                    if (!args.isForceProceed()) {
                        emptyInternal(admin, targetList, sourceServerName, plan, true, args);
                        System.out.println(plan.size() + " regions will be moved.");

                        if (!Util.askProceed()) {
                            return;
                        }
                    }
                    emptyInternal(admin, targetList, sourceServerName, plan, false, args);
                }
            } else {
                System.out.println("No region server is emptied.");
            }
        }

        private void printSourceRSs(List<ServerName> sourceServerNames) {
            int i = 0;
            for (ServerName sourceServerName : sourceServerNames) {
                System.out.println(++i + "/" + sourceServerNames.size()
                    + " - empty - RS - " + sourceServerName.getServerName());
            }
            System.out.println(sourceServerNames.size() + " RSs will be emptied.");
        }

        private void emptyInternal(Admin admin, List<ServerName> targetList, ServerName sourceServerName
            , List<Triple<TableName, String, String>> plan, boolean printPlanOnly, Args args)
            throws IOException, InterruptedException {
            long startTimeStamp = System.currentTimeMillis();
            boolean asynchronous = args.has(Args.OPTION_MOVE_ASYNC);
            if (plan.size() > 0) {
                move(args, admin, sourceServerName, plan, printPlanOnly, asynchronous);
                Util.printVerboseMessage(args, "emptyInternal.move - end", startTimeStamp);

                AssignAction.sleep(args, plan.size());

                // move remained regions cause of splitting or asynchronous move
                if (!printPlanOnly) {
                    int i;
                    for (i = 0; i < Constant.TRY_MAX; i++) {
                        Util.printVerboseMessage(args, "emptyInternal.move.retry - iteration - "
                            + i + " - plan - start");
                        List<Triple<TableName, String, String>> planRemained =
                            plan(args, admin, targetList, sourceServerName);
                        Util.printVerboseMessage(args, "emptyInternal.move.retry - iteration - "
                            + i + " - plan - end", startTimeStamp);
                        if (planRemained.size() == 0) break;

                        System.out.println("There are some regions not moved. Move again.");
                        move(args, admin, sourceServerName, planRemained, printPlanOnly, asynchronous);
                        Util.printVerboseMessage(args, "emptyInternal.move.retry - iteration - "
                            + i + " - move - end", startTimeStamp);

                        Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);

                        if (asynchronous && i > 1) {
                            // assign region again
                            for (Triple<TableName, String, String> triple : planRemained) {
                                try {
                                    Util.printVerboseMessage(args, "emptyInternal.move.retry - iteration - "
                                            + i + " - assign - start");
                                    admin.assign(triple.getRight().getBytes());
                                    Util.printVerboseMessage(args, "emptyInternal.move.retry - iteration - "
                                            + i + " - assign - end", startTimeStamp);
                                } catch (UnknownRegionException ignore) {
                                } catch (RemoteException e) {
                                    if (!e.getMessage().contains("UnknownRegionException"))
                                        throw e;
                                }
                            }
                            Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
                        }
                    }

                    if (i >= Constant.TRY_MAX)
                        throw new IllegalStateException("Cannot empty all regions. Some regions are remained.");
                }
            }
        }

        private void move(Args args, Admin admin, ServerName sourceServerName,
            List<Triple<TableName, String, String>> plan,
            boolean printPlanOnly, boolean asynchronous) throws IOException, InterruptedException {
            int progress = 1;
            for (Triple<TableName, String, String> planEntry : plan) {
                TableName targetTableName = planEntry.getLeft();
                String targetServerName = planEntry.getMiddle();
                String encodedRegionName = planEntry.getRight();
                System.out.print(progress++ + "/" + plan.size() + " - move " + encodedRegionName
                    + " of " + targetTableName
                    + " from " + sourceServerName.getServerName() + " to " + targetServerName);

                if (printPlanOnly) {
                    System.out.println();
                } else {
                    Common.moveWithPrintingResult(args, admin, targetTableName, encodedRegionName, targetServerName,
                        asynchronous);
                }
            }
        }

        private List<Triple<TableName, String, String>> plan(Args args, Admin admin, List<ServerName> targetList,
            ServerName sourceServerName)
            throws IOException {
            long startTimestamp = System.currentTimeMillis();
            List<Triple<TableName, String, String>> plan = new ArrayList<>();

            Util.printVerboseMessage(args, "plan.getOnlineRegions - start");
            List<RegionInfo> onlineRegions = CommandAdapter.getOnlineRegions(args, admin, sourceServerName);
            Util.printVerboseMessage(args, "plan.getOnlineRegions - end", startTimestamp);
            List<ServerName> targetListToMove = new ArrayList<>();
            if (onlineRegions.size() > 0) {
                for (RegionInfo hRegionInfo : onlineRegions) {
                    if (targetListToMove.size() == 0)
                        targetListToMove = new ArrayList<>(targetList);

                    ServerName targetServerName =
                        targetListToMove.remove(new Random().nextInt(targetListToMove.size()));
                    String encodedRegionName = hRegionInfo.getEncodedName();
                    plan.add(new ImmutableTriple<TableName, String, String>(CommandAdapter.getTableName(hRegionInfo),
                        targetServerName.getServerName(), encodedRegionName));
                }
            }
            Util.printVerboseMessage(args, "plan.targetListToMove - end", startTimestamp);

            return plan;
        }

        /**
         * Remove source RS from all of RSs.
         *
         * @param sourceRsRegex  regex of RS names
         * @param targetRSList
         * @return list of removed RSs
         */
        private List<ServerName> removeSource(final String sourceRsRegex, List<ServerName> targetRSList) {
            List<ServerName> toRemove = new ArrayList<>();

            for (ServerName serverName : targetRSList) {
                if (serverName.getServerName().matches(sourceRsRegex)) {
                    toRemove.add(serverName);
                }
            }

            if (toRemove.size() > 0) {
                targetRSList.removeAll(toRemove);
                if (targetRSList.size() == 0)
                    throw new IllegalArgumentException("Cannot empty all RS");

                return toRemove;
            } else
                throw new IllegalArgumentException("invalid RS");
        }
    }, BALANCER {
        @Override
        public void run(Admin admin, Args args) throws IOException {
            String statusStr = ((String) args.getOptionSet().nonOptionArguments().get(2)).toLowerCase();
            final boolean targetStatus;
            switch (statusStr) {
                case "on":
                case "true":
                    targetStatus = true;
                    break;
                case "off":
                case "false":
                    targetStatus = false;
                    break;
                default:
                    throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
            }
            setBalancerRunning(admin, targetStatus);
        }
    }, EXPORT {
        @Override
        public void run(Admin admin, Args args) throws IOException {
            final boolean balancerRunning = isBalancerRunning(admin, args);

            try {
                String fileName = (String) args.getOptionSet().nonOptionArguments().get(2);
                final String regionServerRegex = (String) args.valueOf(Args.OPTION_REGION_SERVER);
                if (args.getOptionSet().nonOptionArguments().size() > 3) {
                    throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                }

                export(args, admin, fileName, regionServerRegex);
            } finally {
                if (balancerRunning) setBalancerRunning(admin, true);
            }
        }
    }, IMPORT {
        @Override
        public void run(Admin admin, Args args) throws IOException, InterruptedException {
            final boolean balancerRunning = isBalancerRunning(admin, args);

            try {
                String fileName = (String) args.getOptionSet().nonOptionArguments().get(2);
                final String regionServerRegex = (String) args.valueOf(Args.OPTION_REGION_SERVER);
                if (args.getOptionSet().nonOptionArguments().size() > 3) {
                    throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                }

                importInternal(admin, fileName, regionServerRegex, args);
            } finally {
                if (balancerRunning) setBalancerRunning(admin, true);
            }
        }

        private void importInternal(Admin admin, String fileName, String regionServerRegex, Args args)
            throws IOException, InterruptedException {
            Map<String, ServerName> serverNameMap = Common.serverNameMap(admin);

            Set<String> importingServers = new HashSet<>();
            List<Triple<TableName, String, String>> assignmentList = new ArrayList<>();
            for (String assignment : Files.readAllLines(Paths.get(fileName), Constant.CHARSET)) {
                String[] split = assignment.split(DELIMITER);
                String serverNameOrg = split[0];
                ServerName serverName = serverNameMap.get(Common.getServerNameKey(serverNameOrg));
                if (serverName == null) {
                    System.out.println("RS " + serverNameOrg + " may be not running or invalid.");
                    continue;
                }
                String serverNameStr = serverName.getServerName();
                String encodedRegionName = split[1];
                TableName tableName = TableName.valueOf(split[2]);

                if (!(serverNameStr == null || regionServerRegex != null
                    && !serverNameStr.matches(removeTimestamp(regionServerRegex)))) {
                    assignmentList.add(new ImmutableTriple<>(tableName, serverNameStr, encodedRegionName));
                    importingServers.add(serverNameStr);
                }
            }

            System.out.println(assignmentList.size() + " regions will be imported. ");
            if (!args.isForceProceed()) {
                if (!Util.askProceed()) {
                    return;
                }
            }

            move(admin, args, assignmentList);
            AssignAction.sleep(args, assignmentList.size());

            retryImport(admin, args, importingServers, assignmentList);
        }

        private void retryImport(Admin admin, Args args, Set<String> importingServers
            , List<Triple<TableName, String, String>> assignmentList) throws IOException, InterruptedException {
            if (!args.has(Args.OPTION_MOVE_ASYNC)) return;
            long startTimestamp = System.currentTimeMillis();

            int i;
            List<Triple<TableName, String, String>> assignmentListRemaining = null;
            for (i = 0; i < Constant.TRY_MAX; i++) {
                assignmentListRemaining = regionsNotImportedYet(args, admin, importingServers, assignmentList);
                if (assignmentListRemaining.size() == 0)
                    break;

                System.out.println("Retry importing");
                move(admin, args, assignmentListRemaining);

                Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
                if (i > 1) {
                    for (Triple<TableName, String, String> triple : assignmentListRemaining) {
                        Util.printVerboseMessage(args, "retryImport - iteration - " +
                                i + " - assign - start");
                        admin.assign(triple.getRight().getBytes());
                        Util.printVerboseMessage(args, "retryImport - iteration - " +
                                i + " - assign - end", startTimestamp);
                    }
                }
                Thread.sleep(Constant.SMALL_WAIT_INTERVAL_MS);
            }
            if (i >= Constant.TRY_MAX) {
                String message = Constant.MESSAGE_CANNOT_MOVE;
                if (assignmentListRemaining != null)
                    message += " - " + assignmentListRemaining.toString();
                throw new IllegalStateException(message);
            }
        }

        private List<Triple<TableName, String, String>> regionsNotImportedYet(
            Args args, Admin admin, Set<String> importingServers,
            List<Triple<TableName, String, String>> assignmentList) throws IOException, InterruptedException {
            List<Triple<TableName, String, String>> assignmentListRemaining = new ArrayList<>();
            Set<String> onlineRegions = new HashSet<>();
            for (String server : importingServers) {
                for (RegionInfo hRegionInfo :
                    CommandAdapter.getOnlineRegions(args, admin, CommandAdapter.create(server))) {
                    onlineRegions.add(hRegionInfo.getEncodedName());
                }
            }

            Map<TableName, Boolean> tableEnabledMap = createTableEnabledMap(args, admin, assignmentList);

            for (Triple<TableName, String, String> triple : assignmentList) {
                if (tableEnabledMap.get(triple.getLeft())) {
                    if (onlineRegions.contains(triple.getRight())) {
                        if (!onlineRegions.contains(triple.getRight()))
                            assignmentListRemaining.add(triple);
                    }
                }
            }
            return assignmentListRemaining;
        }

        // for better performance
        private Map<TableName, Boolean> createTableEnabledMap(Args args, Admin admin,
            List<Triple<TableName, String, String>> assignmentList) throws InterruptedException, IOException {
            Set<TableName> tables = new HashSet<>();
            for (Triple<TableName, String, String> triple : assignmentList) {
                tables.add(triple.getLeft());
            }
            Map<TableName, Boolean> tableEnabledMap = new HashMap<>();
            for (TableName table : tables) {
                tableEnabledMap.put(table, Common.isTableEnabled(args, admin, table));
            }
            return tableEnabledMap;
        }

        private void move(Admin admin, Args args, List<Triple<TableName, String, String>> assignmentList)
            throws IOException, InterruptedException {
            int progress = 1;
            for (Triple<TableName, String, String> assignment : assignmentList) {
                TableName tableName = assignment.getLeft();
                String serverName = assignment.getMiddle();
                String encodedRegionName = assignment.getRight();

                System.out.print(progress++ + "/" + assignmentList.size() + " - move " + encodedRegionName
                    + " of " + tableName + " to " + serverName);
                Common.moveWithPrintingResult(args, admin, tableName, encodedRegionName, serverName
                    , args.has(Args.OPTION_MOVE_ASYNC));
            }
        }

        private String removeTimestamp(String serverName) {
            int timestampStartIndex = serverName.indexOf(',', serverName.indexOf(',', 0) + 1);
            if (timestampStartIndex > -1) {
                return serverName.substring(0, timestampStartIndex) + ".*";
            } else {
                return serverName;
            }
        }
    }, RESTORE {
        @Override
        public void run(Admin admin, Args args) throws IOException, InterruptedException {
            final boolean balancerRunning = isBalancerRunning(admin, args);
            processedCount = 0;

            try {
                List<?> arguments = args.getOptionSet().nonOptionArguments();
                if (arguments.size() != 5) throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                String level = arguments.get(2).toString();
                String regex = arguments.get(3).toString();
                long timestamp = Util.parseTimestamp(arguments.get(4).toString());

                // key: tableName, value: key: encodedRegionName, value: serverNameKey
                Map<TableName, Map<String, String>> versionedRegionMap
                    = CommandAdapter.versionedRegionMap(admin.getConnection(), timestamp);
                Map<String, ServerName> serverNameMap = Common.serverNameMap(admin);
                switch (level.toLowerCase()) {
                    case "table":
                        for (TableName tableName : Args.tables(args, admin, regex)) {
                            progress = 1;
                            System.out.println("Restoring the assignments of table - " + tableName);

                            // encodedRegionName, tableName, serverNameCurrent
                            List<Triple<String, TableName, ServerName>> regionsToMove = new ArrayList<>();

                            for (HRegionLocation regionLocation : admin.getConnection().getRegionLocator(tableName).getAllRegionLocations()) {
                                regionsToMove.add(Triple.of(regionLocation.getRegion().getEncodedName(), tableName,
                                    regionLocation.getServerName()));
                            }
                            regionsToMove = filterRegionsToMove(serverNameMap, versionedRegionMap, regionsToMove);

                            System.out.println(regionsToMove.size() + " regions will be moved. ");
                            System.out.println("Timestamp - " + Constant.DATE_FORMAT.format(timestamp));
                            if (regionsToMove.size() > 0 && !args.isForceProceed()) {
                                if (!Util.askProceed()) {
                                    return;
                                }
                            }

                            moveRegions(admin, args, versionedRegionMap, serverNameMap, regionsToMove);
                        }
                        break;
                    case "rs":
                        for (ServerName serverName : Common.regionServers(admin, regex)) {
                            progress = 1;
                            System.out.println("Restoring the assignments of region server - "
                                + serverName.getServerName());

                            // encodedRegionName, tableName, serverNameCurrent
                            List<Triple<String, TableName, ServerName>> regionsToMove = new ArrayList<>();

                            // find regions in this RS currently
                            for (RegionInfo hRegionInfo : CommandAdapter.getOnlineRegions(args, admin, serverName))
                                regionsToMove.add(Triple.of(hRegionInfo.getEncodedName(),
                                    hRegionInfo.getTable(),
                                    serverName));
                            regionsToMove = filterRegionsToMove(serverNameMap, versionedRegionMap, regionsToMove);

                            // find regions in this RS at that timestamp
                            for (Map.Entry<TableName, Map<String, String>> outerEntry : versionedRegionMap.entrySet()) {
                                for (Map.Entry<String, String> entry : outerEntry.getValue().entrySet()) {
                                    if (serverName.getServerName().startsWith(entry.getValue())) {
                                        TableName tableName = outerEntry.getKey();
                                        String encodedRegionName = entry.getKey();
                                        regionsToMove.add(Triple.of(encodedRegionName, tableName, serverName));
                                    }
                                }
                            }

                            System.out.println(regionsToMove.size() + " regions will be moved. ");
                            System.out.println("Timestamp - " + Constant.DATE_FORMAT.format(timestamp));
                            if (regionsToMove.size() > 0 && !args.isForceProceed()) {
                                if (!Util.askProceed()) {
                                    return;
                                }
                            }

                            moveRegions(admin, args, versionedRegionMap, serverNameMap, regionsToMove);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                }
            } finally {
                if (balancerRunning) setBalancerRunning(admin, true);
            }
        }

        private void moveRegions(Admin admin, Args args, Map<TableName, Map<String, String>> versionedRegionMap,
            Map<String, ServerName> serverNameMap, List<Triple<String, TableName, ServerName>> regionsToMove)
            throws IOException, InterruptedException {
            for (Triple<String, TableName, ServerName> triple : regionsToMove) {
                String encodedRegionName = triple.getLeft();
                TableName tableName = triple.getMiddle();
                ServerName serverNameCur = triple.getRight();
                Map<String, String> regionMap = versionedRegionMap.get(tableName);
                moveRegion(admin, args, serverNameMap, tableName,
                    regionMap, regionsToMove.size(), encodedRegionName, serverNameCur);
            }

            sleep(args, regionsToMove.size());
        }

        private void moveRegion(Admin admin, Args args, Map<String, ServerName> serverNameMap,
            TableName tableName, Map<String, String> regionMap, int maxProgress, String encodedRegionName,
            ServerName serverNameCur)
            throws IOException, InterruptedException {
            System.out.print(progress++ + "/" + maxProgress + " - move " + encodedRegionName
                + " of " + tableName + " from " + serverNameCur);
            String serverNameNew = null;
            String serverNameKey = null;
            if (regionMap != null) {
                serverNameKey = regionMap.get(encodedRegionName);
                if (serverNameKey != null) {
                    serverNameNew = serverNameMap.get(serverNameKey).getServerName();
                    if (serverNameNew != null) {
                        System.out.print(" to " + serverNameNew);
                        if (!Util.askProceedInteractively(args, false)) return;
                        Common.moveWithPrintingResult(args, admin, tableName,
                            encodedRegionName, serverNameNew, args.has(Args.OPTION_MOVE_ASYNC));
                        processedCount++;
                    }
                }
            }
            if (regionMap == null || serverNameKey == null || serverNameNew == null) {
                System.out.println(encodedRegionName
                    + " - SKIPPED - Cannot find any versioned meta record");
            }
        }

        // encodedRegionName, tableName, serverNameCurrent
        private List<Triple<String, TableName, ServerName>> filterRegionsToMove(Map<String, ServerName> serverNameMap,
            Map<TableName, Map<String, String>> versionedRegionMap,
            List<Triple<String, TableName, ServerName>> regionsToMove) {
            List<Triple<String, TableName, ServerName>> result = new ArrayList<>();

            for (Triple<String, TableName, ServerName> triple : regionsToMove) {
                String encodedRegionName = triple.getLeft();
                TableName tableName = triple.getMiddle();
                ServerName serverNameCur = triple.getRight();

                Map<String, String> regionMap = versionedRegionMap.get(tableName);
                if (regionMap != null) {
                    String serverNameKey = regionMap.get(encodedRegionName);
                    if (serverNameKey != null) {
                        ServerName serverNamePrev = serverNameMap.get(serverNameKey);
                        if (serverNamePrev != null) {
                            if (!serverNameCur.equals(serverNamePrev))
                                result.add(Triple.of(encodedRegionName, tableName, serverNameCur));
                        }
                    }
                }
            }

            return result;
        }
    };

    /**
     * Wait for regions to be assigned
     * Waiting time is increased by every 100 regions
     */
    private static void sleep(Args args, int numRegions) throws InterruptedException {
      if (args.has(Args.OPTION_MOVE_ASYNC)) {
        Thread.sleep(Math.max(Constant.LARGE_WAIT_INTERVAL_MS, Constant.SMALL_WAIT_INTERVAL_MS * ((numRegions / 100) + 1)));
      }
    }

    static final String MESSAGE_TURN_BALANCER_OFF = "Turn automatic balancer off.";
    static final String DELIMITER = "/";
    private static int processedCount = 0;
    private static int progress = 1;

    private static void setBalancerRunning(Admin admin, boolean targetStatus) throws IOException {
        boolean balancerRunning = admin.balancerSwitch(targetStatus, true);
        if (targetStatus) {
            if (balancerRunning) {
                System.out.println("Automatic balancer is already turned on.");
            } else {
                System.out.println("Automatic balancer is turned on.");
            }
        } else {
            if (balancerRunning) {
                System.out.println("Automatic balancer is turned off.");
            } else {
                System.out.println("Automatic balancer is already turned off.");
            }
        }
    }

    @VisibleForTesting
    static int getProcessedCount() {
        return processedCount;
    }

    private static boolean isBalancerRunning(Admin admin, Args args) throws IOException {
        final boolean balancerRunning = CommandAdapter.isBalancerRunning(admin);
        final boolean balancerOffOption = args.has(Args.OPTION_TURN_BALANCER_OFF);
        if (balancerRunning) {
            if (balancerOffOption) {
                setBalancerRunning(admin, false);
            } else {
                throw new IllegalStateException(MESSAGE_TURN_BALANCER_OFF);
            }
        }
        return balancerRunning;
    }

    private static void export(Args args, Admin admin, String fileName, String regionServerRegex)
        throws IOException {
        processedCount = 0;

        try (PrintWriter writer = new PrintWriter(fileName, Constant.CHARSET.name())) {
            List<ServerName> serverNameList = Common.regionServers(admin);
            for (ServerName serverName : serverNameList) {
                if (regionServerRegex == null || serverName.getServerName().matches(regionServerRegex)) {
                    for (RegionInfo hRegionInfo : CommandAdapter.getOnlineRegions(args, admin, serverName)) {
                        String assignment = serverName.getServerName() + DELIMITER + hRegionInfo.getEncodedName()
                            + DELIMITER + CommandAdapter.getTableName(hRegionInfo);
                        System.out.println(assignment);
                        writer.println(assignment);
                        processedCount++;
                    }
                }
            }
        }
    }

    public abstract void run(Admin admin, Args args) throws IOException, InterruptedException;
}
