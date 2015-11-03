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

package com.kakao.hbase;

import com.kakao.hbase.common.Args;
import com.kakao.hbase.specific.SnapshotAdapter;
import joptsimple.OptionParser;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import java.io.IOException;
import java.util.*;

public class SnapshotArgs extends Args {
    public static final String COMMA = ",";
    public static final int KEEP_UNLIMITED = 0;
    static final int KEEP_DEFAULT = KEEP_UNLIMITED;
    static final String ENTRY_DELIMITER = "/";

    private final Map<String, Boolean> tableFlushMap = new HashMap<>();
    private final Map<String, Integer> tableKeepMap = new HashMap<>();

    private final Set<String> excludedSet = new HashSet<>();

    public SnapshotArgs(String[] args) throws IOException {
        super(args);

        List nonOptionArguments = optionSet.nonOptionArguments();
        if (nonOptionArguments.size() != 2) {
            throw new IllegalArgumentException("Invalid arguments");
        }
    }

    public boolean isExcluded(String tableName) {
        return excludedSet.contains(tableName);
    }

    public String getZookeeperQuorum() {
        return (String) optionSet.nonOptionArguments().get(0);
    }

    @Override
    protected OptionParser createOptionParser() {
        OptionParser optionParser = createCommonOptionParser();
        optionParser.accepts(OPTION_KEEP).withRequiredArg();
        optionParser.accepts(OPTION_SKIP_FLUSH).withRequiredArg();
        optionParser.accepts(OPTION_EXCLUDE).withRequiredArg();
        optionParser.accepts(OPTION_OVERRIDE).withRequiredArg();
        optionParser.accepts(OPTION_ALERT_SCRIPT).withRequiredArg();
        optionParser.accepts(OPTION_CLEAR_WATCH_LEAK);
        optionParser.accepts(OPTION_CLEAR_WATCH_LEAK_ONLY);
        return optionParser;
    }

    private void override() {
        String[] tables;
        if (optionSet.has(OPTION_OVERRIDE)) {
            tables = ((String) optionSet.valueOf(OPTION_OVERRIDE)).split(COMMA);
            for (String table : tables) {
                String[] parts = table.split(ENTRY_DELIMITER);
                String tableName = parts[0];
                tableKeepMap.put(tableName, Integer.valueOf(parts[1]));
                tableFlushMap.put(tableName, Boolean.valueOf(parts[2]));
            }
        }
    }

    public HBaseProtos.SnapshotDescription.Type flushType(String tableName) {
        if (tableFlushMap.get(tableName) == null) {
            return SnapshotAdapter.getType(optionSet);
        } else {
            return SnapshotAdapter.getType(tableName, tableFlushMap);
        }
    }

    public int keepCount(String tableName) {
        if (tableKeepMap.get(tableName) == null) {
            if (optionSet.has(SnapshotArgs.OPTION_KEEP)) {
                int keepCount = Integer.valueOf((String) optionSet.valueOf(SnapshotArgs.OPTION_KEEP));
                if (keepCount < 0)
                    throw new IllegalArgumentException("keep count should be a positive number.");
                return keepCount;
            } else {
                return KEEP_DEFAULT;
            }
        } else {
            return tableKeepMap.get(tableName);
        }
    }

    public Set<String> tableSet(HBaseAdmin admin) throws IOException {
        Set<String> tableSet = new TreeSet<>();
        String[] tables = ((String) optionSet.nonOptionArguments().get(1)).replaceAll(" ", "").split(COMMA);
        parseTables(admin, tables, tableSet);

        override();

        parseExclude(admin);

        return tableSet;
    }

    private void parseExclude(HBaseAdmin admin) throws IOException {
        if (optionSet.has(OPTION_EXCLUDE)) {
            excludedSet.clear();

            String[] excludes = ((String) optionSet.valueOf(OPTION_EXCLUDE)).split(COMMA);
            parseTables(admin, excludes, excludedSet);
        }
    }

    private void parseTables(HBaseAdmin admin, String[] tables, Set<String> tableSet) throws IOException {
        for (String table : tables) {
            final String tableName;
            if (table.contains(ENTRY_DELIMITER)) {
                String[] parts = table.split(ENTRY_DELIMITER);
                tableName = parts[0];
                tableKeepMap.put(tableName, Integer.valueOf(parts[1]));
                tableFlushMap.put(tableName, Boolean.valueOf(parts[2]));
            } else {
                tableName = table;
            }

            for (HTableDescriptor hTableDescriptor : admin.listTables(tableName)) {
                tableSet.add(hTableDescriptor.getNameAsString());
            }
        }
    }

    public String alertScript() {
        if (optionSet.has(OPTION_ALERT_SCRIPT)) {
            return (String) optionSet.valueOf(OPTION_ALERT_SCRIPT);
        } else {
            return null;
        }
    }
}