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
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.RegionPlan;

import java.io.IOException;
import java.util.*;

@SuppressWarnings("unused")
enum BalanceRule {
    RR {
        @Override
        List<RegionPlan> makePlan(HBaseAdmin admin, Set<String> tableNameSet, Args args) throws IOException {
            List<ServerName> serverNames = getServerNames(admin);
            Map<HRegionInfo, ServerName> regionLocations = Balance.createRegionAssignmentMap(admin, tableNameSet);

            int i = 0;
            List<RegionPlan> regionPlanList = new ArrayList<>();
            for (String tableName : tableNameSet) {
                for (HRegionInfo hRegionInfo : admin.getTableRegions(tableName.getBytes())) {
                    ServerName source = regionLocations.get(hRegionInfo);
                    ServerName dest = serverNames.get((i++) % serverNames.size());
                    if (!source.equals(dest))
                        regionPlanList.add(new RegionPlan(hRegionInfo, source, dest));
                }
            }
            return regionPlanList;
        }
    },
    RD {
        @Override
        List<RegionPlan> makePlan(HBaseAdmin admin, Set<String> tableNameSet, Args args) throws IOException {
            List<ServerName> serverNames = getServerNames(admin);
            List<ServerName> serverNamesToMove = new ArrayList<>(serverNames);
            Map<HRegionInfo, ServerName> regionLocations = Balance.createRegionAssignmentMap(admin, tableNameSet);

            List<RegionPlan> regionPlanList = new ArrayList<>();
            for (String tableName : tableNameSet) {
                for (HRegionInfo hRegionInfo : admin.getTableRegions(tableName.getBytes())) {
                    if (serverNamesToMove.size() == 0)
                        serverNamesToMove = new ArrayList<>(serverNames);

                    ServerName source = regionLocations.get(hRegionInfo);
                    ServerName dest = serverNamesToMove.remove(new Random().nextInt(serverNamesToMove.size()));
                    if (!source.equals(dest))
                        regionPlanList.add(new RegionPlan(hRegionInfo, source, dest));
                }
            }
            return regionPlanList;
        }
    },
    ST {
        @Override
        List<RegionPlan> makePlan(HBaseAdmin admin, Set<String> tableNameSet, Args args) throws IOException {
            return Balance.makePlan(admin, tableNameSet, BalanceFactor.parseArg(args));
        }
    },
    DEFAULT {
        @Override
        List<RegionPlan> makePlan(HBaseAdmin admin, Set<String> tableNameSet, Args args) {
            throw new IllegalStateException("do not call me");
        }
    };

    static List<ServerName> getServerNames(HBaseAdmin admin) throws IOException {
        ClusterStatus clusterStatus = admin.getClusterStatus();
        List<ServerName> serverNameList = new ArrayList<>(clusterStatus.getServers());
        Collections.sort(serverNameList);
        return serverNameList;
    }

    abstract List<RegionPlan> makePlan(HBaseAdmin admin, Set<String> tableNameSet, Args args) throws IOException;
}
