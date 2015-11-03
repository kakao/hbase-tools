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
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Assign implements Command {
    private final HBaseAdmin admin;
    private final Args args;

    public Assign(HBaseAdmin admin, Args args) {
        if (args.getOptionSet().nonOptionArguments().size() < 2) {
            throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
        }

        this.admin = admin;
        this.args = args;
    }

    public static String usage() {
        return "Manage assignment of regions.\n"
                + "usage: " + Assign.class.getSimpleName().toLowerCase() + " <zookeeper quorum> <action> [options]\n"
                + "  actions and options:\n"
                + "    balancer <on or off> : Turn automatic balancer on or off.\n"
                + "    empty <region server regex>: Move all regions out of these region servers.\n"
                + "      --" + Args.OPTION_OUTPUT + "=<export output file>: Export region assignment.\n"
                + "    export <output file>: Export assignment of regions to a file.\n"
                + "      --" + Args.OPTION_REGION_SERVER + "=<region server regex>: Export these region servers only.\n"
                + "    import <input file>: Import assignment of regions from a file.\n"
                + "      --" + Args.OPTION_REGION_SERVER + "=<region server regex>: Import these region servers only.\n"
                + "  options:\n"
                + "    --" + Args.OPTION_TURN_BALANCER_OFF
                + ": Turn automatic balancer off during command is running.\n"
                + "    --" + Args.OPTION_MOVE_ASYNC + ": Move regions asynchronously.\n"
                + Args.commonUsage();
    }

    @Override
    public void run() throws Exception {
        String actionStr = ((String) args.getOptionSet().nonOptionArguments().get(1)).toUpperCase();
        AssignAction assignAction = AssignAction.valueOf(actionStr);
        assignAction.run(admin, args);
    }

    @Override
    public boolean needTableArg() {
        return false;
    }
}
