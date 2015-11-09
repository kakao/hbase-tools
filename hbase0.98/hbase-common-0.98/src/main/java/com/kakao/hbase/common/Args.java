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

package com.kakao.hbase.common;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class Args {
    public static final String OPTION_REGION = "region";
    public static final String OPTION_OUTPUT = "output";
    public static final String OPTION_VERBOSE = "verbose";
    public static final String OPTION_DEBUG = "debug";
    public static final String OPTION_KERBEROS_CONFIG = "krbconf";
    public static final String OPTION_KEY_TAB = "keytab";
    public static final String OPTION_REGION_SERVER = "rs";
    public static final String OPTION_PRINCIPAL = "principal";
    public static final String OPTION_REALM = "realm";
    public static final String OPTION_FORCE_PROCEED = "force-proceed";
    public static final String OPTION_KEEP = "keep";
    public static final String OPTION_SKIP_FLUSH = "skip-flush";
    public static final String OPTION_EXCLUDE = "exclude";
    public static final String OPTION_OVERRIDE = "override";
    public static final String OPTION_AFTER_FAILED = "after-failed";
    public static final String OPTION_AFTER_FINISHED = "after-finished";
    public static final String OPTION_CLEAR_WATCH_LEAK = "clear-watch-leak";
    public static final String OPTION_CLEAR_WATCH_LEAK_ONLY = "clear-watch-leak-only";
    public static final String OPTION_OPTIMIZE = "optimize";
    public static final String OPTION_TURN_BALANCER_OFF = "turn-balancer-off";
    public static final String OPTION_BALANCE_FACTOR = "factor";
    public static final String OPTION_TEST = "test";
    public static final String OPTION_HTTP_PORT = "port";
    public static final String OPTION_INTERVAL = "interval";
    public static final String OPTION_MOVE_ASYNC = "move-async";
    public static final String OPTION_MAX_ITERATION = "max-iteration";

    public static final String INVALID_ARGUMENTS = "Invalid arguments";
    public static final String ALL_TABLES = "";
    private static final int INTERVAL_DEFAULT_MS = 10 * 1000;
    protected final String zookeeperQuorum;
    protected final OptionSet optionSet;

    public Args(String[] args) throws IOException {
        OptionSet optionSetTemp = createOptionParser().parse(args);

        List<?> nonOptionArguments = optionSetTemp.nonOptionArguments();
        if (nonOptionArguments.size() < 1) throw new IllegalArgumentException(INVALID_ARGUMENTS);

        String arg = (String) nonOptionArguments.get(0);

        if (Util.isFile(arg)) {
            String[] newArgs = (String[]) ArrayUtils.addAll(parseArgsFile(arg), Arrays.copyOfRange(args, 1, args.length));
            this.optionSet = createOptionParser().parse(newArgs);
            this.zookeeperQuorum = (String) optionSet.nonOptionArguments().get(0);
        } else {
            this.optionSet = optionSetTemp;
            this.zookeeperQuorum = arg;
        }
    }

    public static String commonUsage() {
        return "  args file:\n"
            + "    Plain text file that contains args and options.\n"
            + "  common options:\n"
            + "    --" + Args.OPTION_FORCE_PROCEED + ": Do not ask whether to proceed.\n"
            + "    --" + Args.OPTION_TEST + ": Set test mode.\n"
            + "    --" + Args.OPTION_DEBUG + ": Print debug log.\n"
            + "    --" + Args.OPTION_VERBOSE + ": Print some more messages.\n"
            + "    --" + Args.OPTION_AFTER_FAILED
            + "=<script> : The script to run when this running is failed.\n"
            + "    --" + Args.OPTION_AFTER_FINISHED
            + "=<script> : The script to run when this running is successfully finished.\n"
            + "    --" + Args.OPTION_KEY_TAB + "=<keytab file>: Kerberos keytab file. Use absolute path.\n"
            + "    --" + Args.OPTION_PRINCIPAL + "=<principal>: Kerberos principal.\n"
            + "    --" + Args.OPTION_REALM + "=<realm>: Kerberos realm to use."
            + " Set this arg if it is not the default realm.\n"
            + "    --" + Args.OPTION_KERBEROS_CONFIG + "=<kerberos config file>: Kerberos config file." +
            " Use absolute path.\n";
    }

    public static String[] parseArgsFile(String fileName) throws IOException {
        return parseArgsFile(fileName, false);
    }

    public static String[] parseArgsFile(String fileName, boolean fromResource) throws IOException {
        final String string;
        if (fromResource) {
            string = Util.readFromResource(fileName);
        } else {
            string = Util.readFromFile(fileName);
        }
        return string.split("[ \n]");
    }

    @Override
    public String toString() {
        if (optionSet == null) return "";

        return (optionSet.nonOptionArguments() == null ? "" : optionSet.nonOptionArguments().toString())
            + " - " + (optionSet.asMap() == null ? "" : optionSet.asMap().toString());
    }

    public String getTableName() {
        if (optionSet.nonOptionArguments().size() > 1) {
            return (String) optionSet.nonOptionArguments().get(1);
        } else {
            return Args.ALL_TABLES;
        }
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    protected abstract OptionParser createOptionParser();

    protected OptionParser createCommonOptionParser() {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts(OPTION_FORCE_PROCEED);
        optionParser.accepts(OPTION_TEST);
        optionParser.accepts(OPTION_DEBUG);
        optionParser.accepts(OPTION_VERBOSE);
        optionParser.accepts(OPTION_KEY_TAB).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_PRINCIPAL).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_REALM).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_KERBEROS_CONFIG).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_AFTER_FAILED).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_AFTER_FINISHED).withRequiredArg().ofType(String.class);
        return optionParser;
    }

    public String getAfterFailedScript() {
        if (optionSet.has(OPTION_AFTER_FAILED)) {
            return (String) optionSet.valueOf(OPTION_AFTER_FAILED);
        } else {
            return null;
        }
    }

    public String getAfterFinishedScript() {
        if (optionSet.has(OPTION_AFTER_FINISHED)) {
            return (String) optionSet.valueOf(OPTION_AFTER_FINISHED);
        } else {
            return null;
        }
    }

    public int getIntervalMS() {
        if (optionSet.has(OPTION_INTERVAL))
            return (Integer) optionSet.valueOf(OPTION_INTERVAL) * 1000;
        else return INTERVAL_DEFAULT_MS;
    }

    public boolean has(String optionName) {
        return optionSet.has(optionName);
    }

    public Object valueOf(String optionName) {
        Object arg = optionSet.valueOf(optionName);
        if (arg != null && arg instanceof String) {
            String argString = ((String) arg).trim();
            return argString.length() == 0 ? null : argString;
        } else {
            return arg;
        }
    }

    public OptionSet getOptionSet() {
        return optionSet;
    }

    public boolean isForceProceed() {
        return optionSet.has(OPTION_FORCE_PROCEED);
    }
}
