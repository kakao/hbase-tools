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

package com.kakao.hbase.stat.print;

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.stat.load.Load;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

public class Formatter {
    private static final LengthMapBuilder LENGTH_MAP_BUILDER = new LengthMapBuilder();
    private static final PrintStringBuilder PRINT_STRING_BUILDER = new PrintStringBuilder();
    private final String tableName;
    private final Load load;
    private Map<Type, String> cache = new HashMap<>();

    public Formatter(String tableName, Load load) {
        this.tableName = tableName;
        this.load = load;
    }

    /**
     * Calculate lengths of strings for formatting
     */
    private Map<String, Length> generateLengthMap(Type formatType) {
        Map<String, Length> entryLengthMap = new HashMap<>();

        for (PrintEntry printEntry : PrintEntry.values())
            printEntry.build(load, null, entryLengthMap, LENGTH_MAP_BUILDER, formatType);

        return entryLengthMap;
    }

    public boolean toggleShowChangedOnly() {
        return load.toggleShowChangedOnly();
    }

    public boolean toggleDiffFromStart() {
        return load.toggleDiffFromStart();
    }

    private java.lang.StringBuilder createRunInformation() {
        java.lang.StringBuilder sb = new java.lang.StringBuilder();

        load.initializeTimestamp();

        sb.append(Util.DATE_FORMAT.format(load.getTimestampIteration()));
        sb.append(" - ").append(load.getTotalDuration()).append(" secs");
        if (tableName.equals(Args.ALL_TABLES)) {
            sb.append(" - All Tables");
        } else {
            sb.append(" - Table: ").append(tableName);
        }
        sb.append(" - DiffFromStart: ").append(load.isDiffFromStart());
        sb.append(" - ShowChangedOnly: ").append(load.isShowChangedOnly());
        sb.append(" - ShowRate: ").append(load.isShowRate());
        sb.append(" - SortKey: ").append(load.getSortKeyInfo());
        sb.append("\n");

        return sb;
    }

    private void appendStat(StringBuilder sb, Type formatType) {
        Map<String, Length> entryLengthMap = generateLengthMap(formatType);

        for (PrintEntry printEntry : PrintEntry.values())
            printEntry.build(load, sb, entryLengthMap, PRINT_STRING_BUILDER, formatType);
    }

    @Override
    public String toString() {
        return buildString(true, Type.ANSI);
    }

    public String toHtmlString() {
        return buildString(true, Type.HTML);
    }

    @VisibleForTesting
    String buildString(boolean withRunInformation, Type formatType) {
        if (load.isUpdating()) {
            return cache.get(formatType);
        } else {
            cache.put(Type.ANSI, buildStringInternal(withRunInformation, Type.ANSI));
            cache.put(Type.HTML, buildStringInternal(withRunInformation, Type.HTML));
            return cache.get(formatType);
        }
    }

    private String buildStringInternal(boolean withRunInformation, Type formatType) {
        StringBuilder sb = new StringBuilder();
        appendStat(sb, formatType);
        if (withRunInformation) sb.insert(0, createRunInformation());
        return sb.toString();
    }

    public enum Type {ANSI, HTML}
}
