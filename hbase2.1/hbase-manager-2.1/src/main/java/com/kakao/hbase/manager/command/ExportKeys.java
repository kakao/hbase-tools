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
import com.kakao.hbase.specific.CommandAdapter;
import com.kakao.hbase.stat.load.TableInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.PrintWriter;
import java.util.Set;

@SuppressWarnings("unused")
public class ExportKeys implements Command {
    static final String DELIMITER = "-";
    private final Admin admin;
    private final Args args;
    private final String outputFileName;
    private final int exportThreshold;

    ExportKeys(Admin admin, Args args) {
        this.admin = admin;
        this.args = args;

        if (args.getTableNamePattern().equals(Args.ALL_TABLES))
            throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);

        if (args.getOptionSet().nonOptionArguments().size() != 3)
            throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
        outputFileName = (String) args.getOptionSet().nonOptionArguments().get(2);

        if (args.has(Args.OPTION_OPTIMIZE)) {
            exportThreshold = parseExportThreshold();
            System.out.println("exporting threshold: " + exportThreshold + " MB");
        } else {
            exportThreshold = 0;
            System.out.println("exporting threshold is not set");
        }
    }

    public static String usage() {
        return "Export startkeys and endkeys of regions of a table in hexstr format.\n"
                + "usage: " + ExportKeys.class.getSimpleName().toLowerCase()
                + " <zookeeper quorum> <table regex> <output file> [options]\n"
                + "  options:" + "\n"
                + "    --optimize=<threshold(MB)> : skip exporting keys of small regions below the threshold" + "\n"
                + Args.commonUsage();
    }

    @Override
    public void run() throws Exception {
        try (PrintWriter writer = new PrintWriter(outputFileName, Constant.CHARSET.name())) {
            writer.print("");

            TableInfo tableInfo = new TableInfo(admin, args.getTableNamePattern(), args);
            tableInfo.refresh();
            Set<RegionInfo> regions = tableInfo.getRegionInfoSet();

            int exported = 0, storeFileSizeMBSum = 0;
            for (RegionInfo regionInfo : regions) {
                System.out.println(regionInfo.toString());

                if (args.getOptionSet().has(Args.OPTION_OPTIMIZE)) {
                    storeFileSizeMBSum += tableInfo.getRegionLoad(regionInfo).getStorefileSizeMB();
                    if (storeFileSizeMBSum < exportThreshold) {
                        continue;
                    }
                }
                if (export(writer, regionInfo)) exported++;
                storeFileSizeMBSum = 0;
            }

            System.out.println("\n" + regions.size() + " regions are scanned.");
            System.out.println(exported + " startkeys are exported.");
            System.out.println("\n" + outputFileName + " file is successfully created.");
        }
    }

    private int parseExportThreshold() {
        String optionValue = (String) args.getOptionSet().valueOf(Args.OPTION_OPTIMIZE);
        if (optionValue.toUpperCase().endsWith("G")) {
            return (int) (Double.valueOf(optionValue.substring(0, optionValue.length() - 1)) * 1024);
        } else if (optionValue.toUpperCase().endsWith("GB")) {
            return (int) (Double.valueOf(optionValue.substring(0, optionValue.length() - 2)) * 1024);
        } else {
            return Double.valueOf(optionValue).intValue();
        }
    }

    private boolean export(PrintWriter writer, RegionInfo regionInfo) {
        TableName tableName = CommandAdapter.getTableName(regionInfo);
        String startKeyHexStr = Bytes.toStringBinary(regionInfo.getStartKey());
        String endKeyHexStr = Bytes.toStringBinary(regionInfo.getEndKey());

        if (startKeyHexStr.length() > 0 || endKeyHexStr.length() > 0) {
            writer.println(tableName + DELIMITER + " " + startKeyHexStr + DELIMITER + " " + endKeyHexStr);
            return true;
        } else {
            return false;
        }
    }
}
