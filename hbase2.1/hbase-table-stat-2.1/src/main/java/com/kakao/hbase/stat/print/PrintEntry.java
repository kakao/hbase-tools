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

import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.specific.RegionLoadAdapter;
import com.kakao.hbase.stat.load.Level;
import com.kakao.hbase.stat.load.Load;
import com.kakao.hbase.stat.load.LoadRecord;
import com.kakao.hbase.stat.load.SortKey;

import java.util.List;
import java.util.Map;

enum PrintEntry {
    header {
        @Override
        public void build(Load load, StringBuilder sb, Map<String, Length> entryLengthMap, Builder builder, Formatter.Type formatType) {
            SortKey sortKey = load.getSortKey();

            String levelTypeString = load.getLevelClass().getLevelTypeString();
            levelTypeString = SortKey.color(sortKey, null).build(levelTypeString, formatType);
            builder.build(entryLengthMap, sb, ENTRY_LEVEL, levelTypeString, "", formatType);

            for (LoadEntry loadEntry : RegionLoadAdapter.loadEntries) {
                String loadEntryName = loadEntry.name();
                loadEntryName = SortKey.color(sortKey, loadEntry).build(loadEntryName, formatType);
                builder.build(entryLengthMap, sb, loadEntryName, loadEntryName, "", formatType);
            }
            if (sb != null) sb.append("\n");
        }
    },
    body {
        @Override
        public void build(Load load, StringBuilder sb, Map<String, Length> entryLengthMap, Builder builder, Formatter.Type formatType) {
            List<Level> sortedLevels = load.sortedLevels();
            for (Level level : sortedLevels) {
                if (load.isShowChangedOnly() && !load.isRecordChanged(level)) continue;

                builder.build(entryLengthMap, sb, ENTRY_LEVEL, Color.LEVEL.build(PADDING + level.toString(), formatType), "", formatType);

                LoadRecord loadRecord = load.getLoadMap().get(level);
                for (LoadEntry loadEntry : RegionLoadAdapter.loadEntries) {
                    String valueString = loadEntry.toString(loadRecord.get(loadEntry));
                    String diffString = load.getValueDiffString(level, loadEntry);
                    if (load.isValueChanged(level, loadEntry)) {
                        valueString = Color.CHANGED.build(valueString, formatType);
                        diffString = Color.CHANGED.build(diffString, formatType);
                    }
                    builder.build(entryLengthMap, sb, loadEntry.name(), valueString, diffString, formatType);
                }
                if (sb != null) sb.append("\n");
            }
        }
    },
    footer {
        @Override
        public void build(Load load, StringBuilder sb, Map<String, Length> entryLengthMap, Builder builder, Formatter.Type formatType) {
            String footerLevelString = getFooterLevelString(load);
            builder.build(entryLengthMap, sb, ENTRY_LEVEL, Color.LEVEL.build(footerLevelString, formatType), "", formatType);
            for (LoadEntry loadEntry : RegionLoadAdapter.loadEntries) {
                Number value = load.getSummary().get(loadEntry);
                String valueString = loadEntry.toString(value);
                String diffString = load.getSummaryDiffString(loadEntry);
                if (load.isSummaryChanged(loadEntry)) {
                    valueString = Color.CHANGED.build(valueString, formatType);
                    diffString = Color.CHANGED.build(diffString, formatType);
                }
                builder.build(entryLengthMap, sb, loadEntry.name(), valueString, diffString, formatType);
            }
            if (sb != null) sb.append("\n");
        }

        private String getFooterLevelString(Load load) {
            return PADDING + "Total: " + load.getLoadMap().size();
        }
    };

    static final String PADDING = " ";
    private static final String ENTRY_LEVEL = "level";

    public abstract void build(Load load, StringBuilder sb, Map<String, Length> entryLengthMap, Builder builder, Formatter.Type formatType);
}
