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

import com.kakao.hbase.common.util.Util;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.util.*;

public abstract class Args {
    public static final String OPTION_REGION = "region";
    public static final String OPTION_OUTPUT = "output";
    public static final String OPTION_SKIP_EXPORT = "skip-export";
    public static final String OPTION_VERBOSE = "verbose";
    static final String OPTION_DEBUG = "debug";
    static final String OPTION_KERBEROS_CONFIG = "krbconf";
    public static final String OPTION_KEY_TAB = "keytab";
    public static final String OPTION_KEY_TAB_SHORT = "k";
    public static final String OPTION_REGION_SERVER = "rs";
    public static final String OPTION_PRINCIPAL = "principal";
    public static final String OPTION_PRINCIPAL_SHORT = "p";
    static final String OPTION_REALM = "realm";
    public static final String OPTION_FORCE_PROCEED = "force-proceed";
    public static final String OPTION_KEEP = "keep";
    public static final String OPTION_DELETE_SNAPSHOT_FOR_NOT_EXISTING_TABLE = "delete-snapshot-for-not-existing-table";
    public static final String OPTION_SKIP_FLUSH = "skip-flush";
    public static final String OPTION_EXCLUDE = "exclude";
    public static final String OPTION_OVERRIDE = "override";
    public static final String OPTION_AFTER_FAILURE = "after-failure";
    public static final String OPTION_AFTER_SUCCESS = "after-success";
    public static final String OPTION_AFTER_FINISH = "after-finish";
    public static final String OPTION_CLEAR_WATCH_LEAK = "clear-watch-leak";
    public static final String OPTION_OPTIMIZE = "optimize";
    public static final String OPTION_TURN_BALANCER_OFF = "turn-balancer-off";
    public static final String OPTION_BALANCE_FACTOR = "factor";
    public static final String OPTION_TEST = "test";    // for test cases only
    public static final String OPTION_HTTP_PORT = "port";
    public static final String OPTION_INTERVAL = "interval";
    public static final String OPTION_MOVE_ASYNC = "move-async";
    public static final String OPTION_MAX_ITERATION = "max-iteration";
    public static final String OPTION_LOCALITY_THRESHOLD = "locality";
    public static final String OPTION_CF = "cf";
    public static final String OPTION_WAIT_UNTIL_FINISH = "wait";
    public static final String OPTION_INTERACTIVE = "interactive";
    private static final String OPTION_CONF = "conf";
    private static final String OPTION_CONF_SHORT = "c";
    public static final String OPTION_PHOENIX = "phoenix-salting-table";

    public static final String INVALID_ARGUMENTS = "Invalid arguments";
    public static final String ALL_TABLES = "";
    private static final int INTERVAL_DEFAULT_MS = 10 * 1000;
    private final String zookeeperQuorum;
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
                + "    --" + Args.OPTION_FORCE_PROCEED + "\n"
                + "        Do not ask whether to proceed.\n"
                + "    --" + Args.OPTION_DEBUG + "\n"
                + "        Print debug log.\n"
                + "    --" + Args.OPTION_VERBOSE + "\n"
                + "        Print some more messages.\n"
                + "    -" + Args.OPTION_CONF_SHORT + "<key=value>, --" + Args.OPTION_CONF + "=<key=value>\n"
                + "        Set a configuration for HBase. Can be used many times for several configurations.\n"
                + "    --" + Args.OPTION_AFTER_FAILURE + "=<script>\n"
                + "        The script to run when this running is failed.\n"
                + "        The first argument of the script should be a message string.\n"
                + "    --" + Args.OPTION_AFTER_SUCCESS + "=<script>\n"
                + "        The script to run when this running is successfully finished.\n"
                + "        The first argument of the script should be a message string.\n"
                + "    --" + Args.OPTION_AFTER_FINISH + "=<script>\n"
                + "        The script to run when this running is successfully finished or failed.\n"
                + "        The first argument of the script should be a message string.\n"
                + "    -" + Args.OPTION_KEY_TAB_SHORT + "<keytab file>, --" + Args.OPTION_KEY_TAB + "=<keytab file>\n"
                + "        Kerberos keytab file. Use absolute path.\n"
                + "    -" + Args.OPTION_PRINCIPAL_SHORT + "<principal>, --" + Args.OPTION_PRINCIPAL + "=<principal>\n"
                + "        Kerberos principal.\n"
                + "    --" + Args.OPTION_REALM + "=<realm>\n"
                + "        Kerberos realm to use. Set this arg if it is not the default realm.\n"
                + "    --" + Args.OPTION_KERBEROS_CONFIG + "=<kerberos config file>\n"
                + "        Kerberos config file. Use absolute path.\n";
    }

    private static String[] parseArgsFile(String fileName) throws IOException {
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

    public static Set<TableName> tables(Args args, Admin admin) throws IOException {
        return tables(args, admin, args.getTableNamePattern());
    }

    @SuppressWarnings("deprecation")
    public static Set<TableName> tables(Args args, Admin admin, String tableNamePattern) throws IOException {
        long startTimestamp = System.currentTimeMillis();
        Util.printVerboseMessage(args, Util.getMethodName() + " - start");
        if (tableNamePattern.equals(ALL_TABLES)) {
            Util.printVerboseMessage(args, Util.getMethodName() + " - end", startTimestamp);
            return Collections.emptySet();
        } else {
            Set<TableName> tables = new TreeSet<>();
            HTableDescriptor[] hTableDescriptors = admin.listTables(tableNamePattern);
            if (hTableDescriptors == null) {
                return tables;
            } else {
                for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                    // fixme
                    // If hbase 1.0 client is connected to hbase 0.98,
                    // admin.listTables(tableName) always returns all tables.
                    // This is a workaround.
                    String nameAsString = hTableDescriptor.getNameAsString();
                    if (nameAsString.matches(tableNamePattern))
                        tables.add(hTableDescriptor.getTableName());
                }
            }

            Util.printVerboseMessage(args, Util.getMethodName() + " - end", startTimestamp);
            return tables;
        }
    }

    @Override
    public String toString() {
        if (optionSet == null) return "";

        StringBuilder nonOptionArgs = new StringBuilder();
        if (optionSet.nonOptionArguments() != null) {
            int i = 0;
            for (Object object : optionSet.nonOptionArguments()) {
                if (i > 0) nonOptionArgs.append(" ");
                nonOptionArgs.append("\"").append(object.toString()).append("\"");
                i++;
            }
        }

        StringBuilder optionArgs = new StringBuilder();
        if (optionSet.asMap() != null) {
            int i = 0;
            for (Map.Entry<OptionSpec<?>, List<?>> entry : optionSet.asMap().entrySet()) {
                if (entry.getValue().size() > 0) {
                    if (i > 0) optionArgs.append(" ");
                    optionArgs.append("--").append(entry.getKey().options().get(0)).append("=\"").append(entry.getValue().get(0)).append("\"");
                    i++;
                }
            }

        }

        return nonOptionArgs + " " + optionArgs;
    }

    public String getTableNamePattern() {
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

    @SuppressWarnings("Duplicates")
    protected OptionParser createCommonOptionParser() {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts(OPTION_FORCE_PROCEED);
        optionParser.accepts(OPTION_TEST);
        optionParser.accepts(OPTION_DEBUG);
        optionParser.accepts(OPTION_VERBOSE);
        optionParser.accepts(OPTION_CONF).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_CONF_SHORT).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_KEY_TAB).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_KEY_TAB_SHORT).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_PRINCIPAL).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_PRINCIPAL_SHORT).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_REALM).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_KERBEROS_CONFIG).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_AFTER_FAILURE).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_AFTER_SUCCESS).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_AFTER_FINISH).withRequiredArg().ofType(String.class);
        return optionParser;
    }

    public Map<String, String> getConfigurations() {
        Map<String, String> result = new HashMap<>();
        if (optionSet.has(OPTION_CONF)) {
            result.putAll(parseConf(OPTION_CONF));
        }
        if (optionSet.has(OPTION_CONF_SHORT)) {
            result.putAll(parseConf(OPTION_CONF_SHORT));
        }
        return result;
    }

    private Map<String, String> parseConf(String option) {
        Map<String, String> result = new HashMap<>();
        List<?> objects = optionSet.valuesOf(option);
        for (Object confArg : objects) {
            String confStr = ((String) confArg);
            int splitPoint = confStr.indexOf("=");
            String key = confStr.substring(0, splitPoint);
            String value = confStr.substring(splitPoint + 1);
            result.put(key, value);
        }
        return result;
    }

    public String getAfterFailureScript() {
        if (optionSet.has(OPTION_AFTER_FAILURE)) {
            return (String) optionSet.valueOf(OPTION_AFTER_FAILURE);
        } else {
            return null;
        }
    }

    public String getAfterSuccessScript() {
        if (optionSet.has(OPTION_AFTER_SUCCESS)) {
            return (String) optionSet.valueOf(OPTION_AFTER_SUCCESS);
        } else {
            return null;
        }
    }

    public String getAfterFinishScript() {
        if (optionSet.has(OPTION_AFTER_FINISH)) {
            return (String) optionSet.valueOf(OPTION_AFTER_FINISH);
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

    public boolean has(String optionLongName, String optionShortName) {
        return optionSet.has(optionLongName) || optionSet.has(optionShortName);
    }

    public Object valueOf(String optionName) {
        Object arg = optionSet.valueOf(optionName);
        if (arg instanceof String) {
            String argString = ((String) arg).trim();
            return argString.length() == 0 ? null : argString;
        } else {
            return arg;
        }
    }

    public Object valueOf(String optionLongName, String optionShortName) {
        Object arg = optionSet.valueOf(optionName(optionLongName, optionShortName));
        if (arg instanceof String) {
            String argString = ((String) arg).trim();
            return argString.length() == 0 ? null : argString;
        } else {
            return arg;
        }
    }

    private String optionName(String optionLongName, String optionShortName) {
        if (has(optionLongName)) return optionLongName;
        else return optionShortName;
    }

    public OptionSet getOptionSet() {
        return optionSet;
    }

    public boolean isForceProceed() {
        return optionSet.has(OPTION_FORCE_PROCEED);
    }

    public String hashStr() {
        return Integer.toHexString(optionSet.hashCode());
    }
}
