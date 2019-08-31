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

package com.kakao.hbase.stat.load;

import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.specific.RegionLoadAdapter;
import com.kakao.hbase.stat.print.Color;

import java.util.*;

public class SortKey {
    public static final SortKey DEFAULT = new SortKey("0");

    private final LoadEntry loadEntry;
    private final ValueEntry valueEntry;

    public SortKey(String sortKeyString) {
        if (sortKeyString == null)
            throw new IllegalArgumentException("sortKeyString should not be null");

        if (sortKeyString.trim().length() != 1)
            throw new IllegalArgumentException("sortKeyString length should be 1");

        if (sortKeyString.matches("^[0-9]")) {
            this.valueEntry = ValueEntry.value;
        } else {
            this.valueEntry = ValueEntry.diff;
        }

        int indexLoadEntry;
        switch (sortKeyString.charAt(0)) {
            case '0':
                indexLoadEntry = -1;
                break;
            case '1':
            case '!':
                indexLoadEntry = 0;
                break;
            case '2':
            case '@':
                indexLoadEntry = 1;
                break;
            case '3':
            case '#':
                indexLoadEntry = 2;
                break;
            case '4':
            case '$':
                indexLoadEntry = 3;
                break;
            case '5':
            case '%':
                indexLoadEntry = 4;
                break;
            case '6':
            case '^':
                indexLoadEntry = 5;
                break;
            case '7':
            case '&':
                indexLoadEntry = 6;
                break;
            case '8':
            case '*':
                indexLoadEntry = 7;
                break;
            case '9':
            case '(':
                indexLoadEntry = 8;
                break;
            default:
                throw new IllegalArgumentException("invalid sortkey string");
        }

        if (indexLoadEntry >= 0) {
            loadEntry = Arrays.asList(RegionLoadAdapter.loadEntries).get(indexLoadEntry);
        } else {
            loadEntry = null;
        }
    }

    private static List<Level> generateSortedLevels(Map<Level, Number> loadMap, final LoadEntry loadEntry) {
        List<Map.Entry<Level, Number>> list = new LinkedList<>(loadMap.entrySet());
        list.sort((o1, o2) -> {
            int compare = loadEntry.compare(o1.getValue(), o2.getValue());
            if (compare == 0) {
                return o1.getKey().toString().compareTo(o2.getKey().toString());
            } else {
                return compare;
            }
        });

        List<Level> result = new ArrayList<>();
        for (Map.Entry<Level, Number> entry : list) {
            result.add(entry.getKey());
        }
        return result;
    }

    public static Color color(SortKey sortKey, LoadEntry loadEntry) {
        if (sortKey == null) {
            if (loadEntry == null) {
                return Color.HEADER_SORTED_BY_VALUE;
            } else {
                return Color.HEADER_DEFAULT;
            }
        } else {
            if (sortKey.loadEntry == loadEntry) {
                if (sortKey.valueEntry == ValueEntry.value) {
                    return Color.HEADER_SORTED_BY_VALUE;
                } else {
                    return Color.HEADER_SORTED_BY_DIFF;
                }
            } else {
                return Color.HEADER_DEFAULT;
            }
        }
    }

    public LoadEntry getLoadEntry() {
        return loadEntry;
    }

    ValueEntry getValueEntry() {
        return valueEntry;
    }

    public List<Level> sortedLevels(Load load) {
        return valueEntry.sortedLevels(this, load);
    }

    @Override
    public String toString() {
        if (loadEntry == null) {
            return valueEntry.name();
        } else {
            return loadEntry.name() + "." + valueEntry.name();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SortKey sortKey = (SortKey) o;

        return loadEntry == sortKey.loadEntry && valueEntry == sortKey.valueEntry;

    }

    @Override
    public int hashCode() {
        int result = loadEntry != null ? loadEntry.hashCode() : 0;
        result = 31 * result + (valueEntry.hashCode());
        return result;
    }

    enum ValueEntry {
        value {
            @Override
            public List<Level> sortedLevels(SortKey sortKey, Load load) {
                Map<Level, Number> valueMap = new HashMap<>();
                for (Map.Entry<Level, LoadRecord> loadRecordEntry : load.getLoadMap().entrySet()) {
                    Level level = loadRecordEntry.getKey();
                    Number value = loadRecordEntry.getValue().get(sortKey.getLoadEntry());
                    valueMap.put(level, value);
                }
                return generateSortedLevels(valueMap, sortKey.getLoadEntry());
            }
        },
        diff {
            @Override
            public List<Level> sortedLevels(SortKey sortKey, Load load) {
                Map<Level, Number> valueMap = new HashMap<>();
                for (Map.Entry<Level, LoadRecord> loadRecordEntry : load.getLoadMap().entrySet()) {
                    Level level = loadRecordEntry.getKey();
                    Number valueDiff = load.getValueDiff(level, sortKey.getLoadEntry());
                    valueMap.put(level, valueDiff == null ? 0 : valueDiff);
                }
                return generateSortedLevels(valueMap, sortKey.getLoadEntry());
            }
        };

        public abstract List<Level> sortedLevels(SortKey sortKey, Load load);
    }
}
