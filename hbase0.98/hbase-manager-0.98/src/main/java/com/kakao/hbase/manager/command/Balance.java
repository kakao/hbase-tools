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

import com.kakao.hbase.ManagerArgs;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unused")
public class Balance implements Command {
    private static Map<HRegionInfo, ServerName> regionLocations = null;
    private final HBaseAdmin admin;
    private final Args args;
    private final String ruleParam;
    private final Set<String> tableNameSet;

    public Balance(HBaseAdmin admin, Args args) throws IOException {
        if (args.getOptionSet().nonOptionArguments().size() != 3) {
            throw new RuntimeException(Args.INVALID_ARGUMENTS);
        }

        this.admin = admin;
        this.args = args;
        ruleParam = (String) args.getOptionSet().nonOptionArguments().get(2);

        tableNameSet = Util.parseTableSet(admin, args);
        reset();
    }

    static void reset() {
        regionLocations = null;
    }

    public static String usage() {
        return "Balance regions evenly by one of the rules below. Move regions one by one except for default.\n"
                + "usage: " + Balance.class.getSimpleName().toLowerCase() + " [options] <zookeeper quorum>" +
                " <table name(regex)> <rule>\n"
                + "  rule:\n"
                + "    default  - hbase default balancer. asynchronous\n"
                + "    rr       - round robin\n"
                + "    rd       - random\n"
                + "    st       - stochastic load balancer\n"
                + "  options:\n"
                + "    --" + ManagerArgs.OPTION_TURN_BALANCER_OFF + ": During balancing turn balancer off.\n"
                + "    --" + ManagerArgs.OPTION_BALANCE_FACTOR + "=<factor>:" +
                " Stochastic load balancer will balance by this single highly weighted factor.\n"
                + "    --" + Args.OPTION_MOVE_ASYNC + ": Move regions asynchronously.\n"
                + "  factors:\n"
                + BalanceFactor.usage(4)
                + Args.commonUsage();
    }

    // does not contain catalog tables
    public static Map<HRegionInfo, ServerName> createRegionAssignmentMap(HBaseAdmin admin, Set<String> tableNameSet) throws IOException {
        if (regionLocations == null) {
            regionLocations = new HashMap<>();
            for (String tableName : tableNameSet) {
                try (HTable table = new HTable(admin.getConfiguration(), tableName)) {
                    regionLocations.putAll(table.getRegionLocations());
                }
            }
        }
        return regionLocations;
    }

    public static List<RegionPlan> makePlan(HBaseAdmin admin, Set<String> tableNameSet, BalanceFactor balanceFactor) throws IOException {
        Map<ServerName, List<HRegionInfo>> clusterState = CommandAdapter.initializeRegionMap(admin);

        for (Map.Entry<HRegionInfo, ServerName> entry : createRegionAssignmentMap(admin, tableNameSet).entrySet())
            clusterState.get(entry.getValue()).add(entry.getKey());

        Configuration conf = admin.getConfiguration();
        conf.setFloat("hbase.regions.slop", 0f);
        balanceFactor.setConf(conf);

        return CommandAdapter.makePlan(admin, clusterState, conf);
    }

    @Override
    public void run() throws Exception {
        boolean balancerRunning = false;

        try {
            balancerRunning = turnBalancerOff();

            BalanceRule rule = BalanceRule.valueOf(ruleParam.toUpperCase());
            if (rule.equals(BalanceRule.DEFAULT)) {
                if (!args.isForceProceed()) {
                    if (!Util.askProceed()) {
                        return;
                    }
                }
                admin.balancer();
                System.out.println("Run hbase default balancer. This is an asynchronous operation.");
            } else {
                List<RegionPlan> regionPlanList = rule.makePlan(admin, tableNameSet, args);
                BalanceFactor.printFactor(BalanceFactor.parseArg(args));

                boolean asynchronous = args.has(Args.OPTION_MOVE_ASYNC);
                if (preview(regionPlanList, asynchronous))
                    balance(regionPlanList, Phase.BALANCE, asynchronous);
            }
        } finally {
            if (balancerRunning) {
                // turn balancer on if needed
                admin.setBalancerRunning(true, true);
                System.out.println("Turn balancer on.");
            }
        }
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private boolean preview(List<RegionPlan> regionPlanList, boolean asynchronous) throws IOException, InterruptedException {
        final boolean proceed;
        if (args.isForceProceed()) {
            proceed = true;
        } else {
            balance(regionPlanList, Phase.PREVIEW, asynchronous);
            if (regionPlanList.size() > 0) {
                System.out.println(regionPlanList.size() + " of " + createRegionAssignmentMap(admin, tableNameSet).size() + " region(s) will be moved.");
                warnBalanceAgain(regionPlanList);
                proceed = Util.askProceed();
            } else {
                System.out.println("There is no region to move.");
                proceed = false;
            }
        }

        return proceed;
    }

    private boolean warnBalanceAgain(List<RegionPlan> regionPlanList) throws IOException {
        List<RegionPlan> allTablePlanList = CommandAdapter.makePlan(admin, regionPlanList);
        if (allTablePlanList != null && allTablePlanList.size() > 0) {
            System.out.println("Warning - Default load balancer will balance the cluster again. " + allTablePlanList.size() + " regions may be re-balanced.");
            return true;
        } else
            return false;
    }

    private boolean turnBalancerOff() throws IOException {
        if (args.getOptionSet().has(ManagerArgs.OPTION_TURN_BALANCER_OFF)) {
            boolean balancerRunning;
            balancerRunning = admin.setBalancerRunning(false, true);
            if (balancerRunning) System.out.println("Turn balancer off");
            return balancerRunning;
        } else {
            return false;
        }
    }

    @SuppressWarnings("deprecation")
    private void balance(List<RegionPlan> regionPlanList, Phase phase, boolean asynchronous) throws IOException, InterruptedException {
        int progress = 1;
        for (RegionPlan regionPlan : regionPlanList) {
            String tableName = Bytes.toString(regionPlan.getRegionInfo().getTableName());
            String encodedRegionName = regionPlan.getRegionInfo().getEncodedName();
            String serverNameDest = regionPlan.getDestination().getServerName();
            String serverNameSource = regionPlan.getSource().getServerName();
            String planStr = progress++ + "/" + regionPlanList.size() + " - move " + encodedRegionName + " of " + tableName + " from " + serverNameSource + " to " + serverNameDest;
            if (phase == Phase.BALANCE) {
                System.out.print(planStr);
            } else {
                System.out.println(planStr);
            }

            if (phase == Phase.BALANCE) {
                Common.moveWithPrintingResult(admin, tableName, encodedRegionName, serverNameDest, asynchronous);
            }
        }

        if (asynchronous && phase == Phase.BALANCE)
            Thread.sleep(Constant.MOVE_ASYNC_WAITING_TIME_MS);
    }

    @Override
    public boolean needTableArg() {
        return false;
    }

    enum Phase {BALANCE, PREVIEW}
}
