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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

import java.io.IOException;
import java.util.*;

public class SnapshotArgs extends Args {
    public static final String COMMA = ",";
    public static final int KEEP_UNLIMITED = 0;
    static final int KEEP_DEFAULT = KEEP_UNLIMITED;
    static final String ENTRY_DELIMITER = "/";

    private final Map<TableName, Boolean> tableFlushMap = new HashMap<>();
    private final Map<TableName, Integer> tableKeepMap = new HashMap<>();

    private final Set<TableName> excludedSet = new HashSet<>();

    public SnapshotArgs(String[] args) throws IOException {
        super(args);

        List nonOptionArguments = optionSet.nonOptionArguments();
        if (nonOptionArguments.size() != 2) {
            throw new IllegalArgumentException("Invalid arguments");
        }
    }

    public boolean isExcluded(TableName tableName) {
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
        optionParser.accepts(OPTION_CLEAR_WATCH_LEAK);
        optionParser.accepts(OPTION_DELETE_SNAPSHOT_FOR_NOT_EXISTING_TABLE);
        return optionParser;
    }

    private void override() {
        String[] tables;
        if (optionSet.has(OPTION_OVERRIDE)) {
            tables = ((String) optionSet.valueOf(OPTION_OVERRIDE)).split(COMMA);
            for (String table : tables) {
                String[] parts = table.split(ENTRY_DELIMITER);
                String tableName = parts[0];
                tableKeepMap.put(TableName.valueOf(tableName), Integer.valueOf(parts[1]));
                tableFlushMap.put(TableName.valueOf(tableName), Boolean.valueOf(parts[2]));
            }
        }
    }

    public SnapshotType flushType(TableName tableName) {
        if (tableFlushMap.get(tableName) == null) {
            return SnapshotAdapter.getType(optionSet);
        } else {
            return SnapshotAdapter.getType(tableName, tableFlushMap);
        }
    }

    public int keepCount(TableName tableName) {
        if (tableKeepMap.get(tableName) == null) {
            if (optionSet.has(SnapshotArgs.OPTION_KEEP)) {
                int keepCount = Integer.valueOf((String) optionSet.valueOf(SnapshotArgs.OPTION_KEEP));
                if (keepCount < 1)
                    throw new IllegalArgumentException("keep count should be a positive number.");
                return keepCount;
            } else {
                return KEEP_DEFAULT;
            }
        } else {
            return tableKeepMap.get(tableName);
        }
    }

    public Set<TableName> tableSet(Admin admin) throws IOException {
        Set<TableName> tableSet = new TreeSet<>();
        String[] tables = ((String) optionSet.nonOptionArguments().get(1)).replaceAll(" ", "").split(COMMA);
        parseTables(admin, tables, tableSet);

        override();

        parseExclude(admin);

        return tableSet;
    }

    private void parseExclude(Admin admin) throws IOException {
        if (optionSet.has(OPTION_EXCLUDE)) {
            excludedSet.clear();

            String[] excludes = ((String) optionSet.valueOf(OPTION_EXCLUDE)).split(COMMA);
            parseTables(admin, excludes, excludedSet);
        }
    }

    private void parseTables(Admin admin, String[] tables, Set<TableName> tableSet) throws IOException {
        for (String table : tables) {
            final String tableName;
            if (table.contains(ENTRY_DELIMITER)) {
                String[] parts = table.split(ENTRY_DELIMITER);
                tableName = parts[0];
                tableKeepMap.put(TableName.valueOf(tableName), Integer.valueOf(parts[1]));
                tableFlushMap.put(TableName.valueOf(tableName), Boolean.valueOf(parts[2]));
            } else {
                tableName = table;
            }

            for (HTableDescriptor hTableDescriptor : admin.listTables(tableName)) {
                tableSet.add(hTableDescriptor.getTableName());
            }
        }
    }
}
