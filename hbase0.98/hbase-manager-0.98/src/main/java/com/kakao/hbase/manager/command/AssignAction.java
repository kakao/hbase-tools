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
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("unused")
enum AssignAction {
    EMPTY {
        @Override
        public void run(HBaseAdmin admin, Args args) throws IOException, InterruptedException {
            final boolean balancerRunning = isBalancerRunning(admin, args);

            try {
                List<ServerName> targetList = new ArrayList<>(admin.getClusterStatus().getServers());
                String sourceRsRegex = (String) args.getOptionSet().nonOptionArguments().get(2);
                final String expFileName = (String) args.valueOf(Args.OPTION_OUTPUT);
                if (args.getOptionSet().nonOptionArguments().size() > 3) {
                    throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                }

                empty(admin, args, targetList, sourceRsRegex, expFileName);
            } finally {
                if (balancerRunning) setBalancerRunning(admin, true);
            }
        }

        private void empty(HBaseAdmin admin, Args args, List<ServerName> targetList, String sourceRsRegex, String expFileName) throws IOException, InterruptedException {
            List<ServerName> sourceServerNames = removeSource(sourceRsRegex, targetList);

            if (sourceServerNames.size() > 0) {
                printSourceRSs(sourceServerNames);
                if (!args.isForceProceed()) {
                    if (!Util.askProceed()) {
                        return;
                    }
                }

                if (expFileName != null) {
                    export(admin, expFileName, sourceRsRegex);
                }

                for (ServerName sourceServerName : sourceServerNames) {
                    List<Triple<String, String, String>> plan = plan(admin, targetList, sourceServerName);

                    if (!args.isForceProceed()) {
                        emptyInternal(admin, targetList, sourceServerName, plan, true, args.has(Args.OPTION_MOVE_ASYNC));
                        System.out.println(plan.size() + " regions will be moved.");

                        if (!Util.askProceed()) {
                            return;
                        }
                    }
                    emptyInternal(admin, targetList, sourceServerName, plan, false, args.has(Args.OPTION_MOVE_ASYNC));
                }
            } else {
                System.out.println("No region server is emptied.");
            }
        }

        private void printSourceRSs(List<ServerName> sourceServerNames) {
            int i = 0;
            for (ServerName sourceServerName : sourceServerNames) {
                System.out.println(++i + "/" + sourceServerNames.size() + " - empty - RS - " + sourceServerName.getServerName());
            }
            System.out.println(sourceServerNames.size() + " RSs will be emptied.");
        }

        private void emptyInternal(HBaseAdmin admin, List<ServerName> targetList, ServerName sourceServerName
                , List<Triple<String, String, String>> plan, boolean printPlanOnly, boolean asynchronous) throws IOException, InterruptedException {
            if (plan.size() > 0) {
                move(admin, sourceServerName, plan, printPlanOnly, asynchronous);

                // move remained regions cause of splitting or asynchronous move
                if (!printPlanOnly) {
                    int i;
                    for (i = 0; i < Constant.TRY_MAX; i++) {
                        List<Triple<String, String, String>> planRemained = plan(admin, targetList, sourceServerName);
                        if (planRemained.size() == 0) break;

                        System.out.println("There are some regions not moved. Move again.");
                        move(admin, sourceServerName, planRemained, printPlanOnly, asynchronous);

                        Thread.sleep(Constant.WAIT_INTERVAL_MS);

                        if (asynchronous && i > 1) {
                            // assign region again
                            for (Triple<String, String, String> triple : planRemained) {
                                try {
                                    admin.assign(triple.getRight().getBytes());
                                } catch (UnknownRegionException ignore) {
                                }
                            }
                            Thread.sleep(Constant.WAIT_INTERVAL_MS);
                        }
                    }

                    if (i >= Constant.TRY_MAX)
                        throw new IllegalStateException("Cannot empty all regions. Some regions are remained.");
                }
            }
        }

        private void move(HBaseAdmin admin, ServerName sourceServerName, List<Triple<String, String, String>> plan
                , boolean printPlanOnly, boolean asynchronous) throws IOException, InterruptedException {
            int progress = 1;
            for (Triple<String, String, String> planEntry : plan) {
                String targetTableName = planEntry.getLeft();
                String targetServerName = planEntry.getMiddle();
                String encodedRegionName = planEntry.getRight();
                System.out.print(progress++ + "/" + plan.size() + " - move " + encodedRegionName
                        + " of " + targetTableName
                        + " from " + sourceServerName.getServerName() + " to " + targetServerName);

                if (printPlanOnly) {
                    System.out.println();
                } else {
                    Common.moveWithPrintingResult(admin, targetTableName, encodedRegionName, targetServerName, asynchronous);
                }
            }
        }

        private List<Triple<String, String, String>> plan(HBaseAdmin admin, List<ServerName> targetList, ServerName sourceServerName) throws IOException, InterruptedException {
            List<Triple<String, String, String>> plan = new ArrayList<>();

            List<HRegionInfo> onlineRegions = CommandAdapter.getOnlineRegions(admin, sourceServerName);
            List<ServerName> targetListToMove = new ArrayList<>();
            if (onlineRegions.size() > 0) {
                for (HRegionInfo hRegionInfo : onlineRegions) {
                    if (targetListToMove.size() == 0)
                        targetListToMove = new ArrayList<>(targetList);

                    ServerName targetServerName = targetListToMove.remove(new Random().nextInt(targetListToMove.size()));
                    String encodedRegionName = hRegionInfo.getEncodedName();
                    plan.add(new ImmutableTriple<>(CommandAdapter.getTableName(hRegionInfo), targetServerName.getServerName(), encodedRegionName));
                }
            }

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
        public void run(HBaseAdmin admin, Args args) throws IOException {
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
        public void run(HBaseAdmin admin, Args args) throws IOException, InterruptedException {
            final boolean balancerRunning = isBalancerRunning(admin, args);

            try {
                String fileName = (String) args.getOptionSet().nonOptionArguments().get(2);
                final String regionServerRegex = (String) args.valueOf(Args.OPTION_REGION_SERVER);
                if (args.getOptionSet().nonOptionArguments().size() > 3) {
                    throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
                }

                export(admin, fileName, regionServerRegex);
            } finally {
                if (balancerRunning) setBalancerRunning(admin, true);
            }
        }
    }, IMPORT {
        @Override
        public void run(HBaseAdmin admin, Args args) throws IOException, InterruptedException {
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

        private void importInternal(HBaseAdmin admin, String fileName, String regionServerRegex, Args args)
                throws IOException, InterruptedException {
            Map<String, ServerName> serverNameMap = new HashMap<>();
            for (ServerName serverName : admin.getClusterStatus().getServers()) {
                serverNameMap.put(getServerNameKey(serverName.getServerName()), serverName);
            }

            Set<String> importingServers = new HashSet<>();
            List<Triple<String, String, String>> assignmentList = new ArrayList<>();
            for (String assignment : Files.readAllLines(Paths.get(fileName), Constant.CHARSET)) {
                String[] split = assignment.split(DELIMITER);
                String serverNameOrg = split[0];
                ServerName serverName = serverNameMap.get(getServerNameKey(serverNameOrg));
                if (serverName == null) {
                    System.out.println("RS " + serverNameOrg + " may be not running or invalid.");
                    continue;
                }
                String serverNameStr = serverName.getServerName();
                String encodedRegionName = split[1];
                String tableName = split[2];

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
            Thread.sleep(Constant.WAIT_INTERVAL_MS);

            retryImport(admin, args, importingServers, assignmentList);
        }

        private void retryImport(HBaseAdmin admin, Args args, Set<String> importingServers
                , List<Triple<String, String, String>> assignmentList) throws IOException, InterruptedException {
            if (!args.has(Args.OPTION_MOVE_ASYNC)) return;

            int i;
            List<Triple<String, String, String>> assignmentListRemaining = null;
            for (i = 0; i < Constant.TRY_MAX; i++) {
                assignmentListRemaining = regionsNotImportedYet(admin, importingServers, assignmentList);
                if (assignmentListRemaining.size() == 0)
                    break;

                System.out.println("Retry importing");
                move(admin, args, assignmentListRemaining);

                Thread.sleep(Constant.WAIT_INTERVAL_MS);
                if (i > 1) {
                    for (Triple<String, String, String> triple : assignmentListRemaining) {
                        admin.assign(triple.getRight().getBytes());
                    }
                }
                Thread.sleep(Constant.WAIT_INTERVAL_MS);
            }
            if (i >= Constant.TRY_MAX) {
                String message = Constant.MESSAGE_CANNOT_MOVE;
                if (assignmentListRemaining != null)
                    message += " - " + assignmentListRemaining.toString();
                throw new IllegalStateException(message);
            }
        }

        @SuppressWarnings("deprecation")
        private List<Triple<String, String, String>> regionsNotImportedYet(
                HBaseAdmin admin, Set<String> importingServers
                , List<Triple<String, String, String>> assignmentList) throws IOException, InterruptedException {
            List<Triple<String, String, String>> assignmentListRemaining = new ArrayList<>();
            Set<String> onlineRegions = new HashSet<>();
            for (String server : importingServers) {
                for (HRegionInfo hRegionInfo : CommandAdapter.getOnlineRegions(admin, new ServerName(server))) {
                    onlineRegions.add(hRegionInfo.getEncodedName());
                }
            }
            for (Triple<String, String, String> triple : assignmentList) {
                if (Common.isTableEnabled(admin, triple.getLeft())) {
                    if (onlineRegions.contains(triple.getRight())) {
                        if (!onlineRegions.contains(triple.getRight()))
                            assignmentListRemaining.add(triple);
                    }
                }
            }
            return assignmentListRemaining;
        }

        private void move(HBaseAdmin admin, Args args, List<Triple<String, String, String>> assignmentList)
                throws IOException, InterruptedException {
            int progress = 1;
            for (Triple<String, String, String> assignment : assignmentList) {
                String tableName = assignment.getLeft();
                String serverName = assignment.getMiddle();
                String encodedRegionName = assignment.getRight();

                System.out.print(progress++ + "/" + assignmentList.size() + " - move " + encodedRegionName
                        + " of " + tableName + " to " + serverName);
                Common.moveWithPrintingResult(admin, tableName, encodedRegionName, serverName
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

        private String getServerNameKey(String serverNameOrg) {
            return serverNameOrg.split(",")[0] + "," + serverNameOrg.split(",")[1];
        }
    };

    static final String MESSAGE_TURN_BALANCER_OFF = "Turn automatic balancer off.";
    static final String DELIMITER = "/";
    private static int exportCount = 0;

    private static void setBalancerRunning(HBaseAdmin admin, boolean targetStatus) throws IOException {
        boolean balancerRunning = admin.setBalancerRunning(targetStatus, true);
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
    static int getExportCount() {
        return exportCount;
    }

    private static boolean isBalancerRunning(HBaseAdmin admin, Args args) throws IOException {
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

    private static void export(HBaseAdmin admin, String fileName, String regionServerRegex) throws IOException {
        exportCount = 0;

        try (PrintWriter writer = new PrintWriter(fileName, Constant.CHARSET.name())) {
            List<ServerName> serverNameList = new ArrayList<>(admin.getClusterStatus().getServers());
            for (ServerName serverName : serverNameList) {
                if (regionServerRegex == null || serverName.getServerName().matches(regionServerRegex)) {
                    for (HRegionInfo hRegionInfo : CommandAdapter.getOnlineRegions(admin, serverName)) {
                        String assignment = serverName.getServerName() + DELIMITER + hRegionInfo.getEncodedName()
                                + DELIMITER + CommandAdapter.getTableName(hRegionInfo);
                        System.out.println(assignment);
                        writer.println(assignment);
                        exportCount++;
                    }
                }
            }
        }
    }

    public abstract void run(HBaseAdmin admin, Args args) throws IOException, InterruptedException;
}
