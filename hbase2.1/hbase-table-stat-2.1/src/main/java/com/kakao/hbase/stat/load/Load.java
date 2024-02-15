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

import com.google.common.annotations.VisibleForTesting;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import com.kakao.hbase.specific.RegionLoadAdapter;
import com.kakao.hbase.specific.RegionLoadDelegator;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.util.*;

public class Load {
    public static final long EMPTY_TIMESTAMP = 0;
    private final LevelClass levelClass;
    private final LoadIO loadIO = new LoadIO(this);
    private final Map<Level, LoadRecord> valueChangeMap = new HashMap<>();
    private final Map<Level, Boolean> recordChangeMap = new HashMap<>();
    private final Args args;
    private Map<Level, LoadRecord> loadMap = new TreeMap<>();
    private Map<Level, LoadRecord> loadMapPrev = new TreeMap<>();
    private Map<Level, LoadRecord> loadMapStart = null;
    private LoadRecord summary = new LoadRecord();
    private LoadRecord summaryPrev = new LoadRecord();
    private LoadRecord summaryStart = null;
    private LoadRecord summaryChangeMap = new LoadRecord();
    private long timestampStart = EMPTY_TIMESTAMP;
    private long timestampIteration = EMPTY_TIMESTAMP;
    private long timestampIterationPrev = EMPTY_TIMESTAMP;
    private boolean diffFromStart = false;
    private boolean showChangedOnly = false;
    private SortKey sortKey = SortKey.DEFAULT;
    private boolean showRate = false;
    private boolean isUpdating = false;

    Load(LevelClass levelClass, Args args) {
        this.levelClass = levelClass;
        this.args = args;
    }

    @VisibleForTesting
    public Load(LevelClass levelClass) {
        this(levelClass, null);
    }

    public boolean isUpdating() {
        return isUpdating;
    }

    void setIsUpdating(boolean isUpdating) {
        this.isUpdating = isUpdating;
    }

    public void resetDiffStartPoint() {
        timestampStart = EMPTY_TIMESTAMP;
        timestampIteration = EMPTY_TIMESTAMP;
        loadMap = new TreeMap<>();
        loadMapPrev = new TreeMap<>();
        loadMapStart = null;
        summary = new LoadRecord();
        summaryPrev = new LoadRecord();
        summaryStart = null;
    }

    private long getDuration() {
        if (isDiffFromStart()) {
            return timestampIteration - timestampStart;
        } else {
            return timestampIteration - timestampIterationPrev;
        }
    }

    @VisibleForTesting
    public void setDuration(long duration) {
        timestampStart = EMPTY_TIMESTAMP;
        timestampIteration = timestampStart + duration;
        timestampIterationPrev = timestampStart;
    }

    public LevelClass getLevelClass() {
        return levelClass;
    }

    public void initializeTimestamp() {
        if (timestampStart == EMPTY_TIMESTAMP) timestampStart = timestampIteration;
    }

    public long getTimestampIteration() {
        return timestampIteration;
    }

    public long getTotalDuration() {
        return Math.round((timestampIteration - timestampStart) / 1000.0);
    }

    private void resetTimestamp() {
        timestampIterationPrev = timestampIteration;
        timestampIteration = System.currentTimeMillis();
    }

    public long getTimestampStart() {
        return timestampStart;
    }

    public boolean toggleShowRate() {
        return showRate = !showRate;
    }

    public boolean isShowRate() {
        return showRate;
    }

    public boolean isValueChanged(Level level, LoadEntry loadEntry) {
        LoadRecord loadRecord = valueChangeMap.get(level);
        if (loadRecord == null) {
            return false;
        } else {
            Number changed = loadRecord.get(loadEntry);
            return (changed != null) && (changed.intValue() == ChangeState.changed.ordinal());
        }
    }

    public boolean isRecordChanged(Level level) {
        Boolean changed = recordChangeMap.get(level);
        return changed == null ? true : changed;
    }

    public boolean isSummaryChanged(LoadEntry loadEntry) {
        Number number = summaryChangeMap.get(loadEntry);
        return number != null && number.equals(ChangeState.changed.ordinal());
    }

    public boolean isDiffFromStart() {
        return diffFromStart;
    }

    public boolean toggleDiffFromStart() {
        return diffFromStart = !diffFromStart;
    }

    public boolean isShowChangedOnly() {
        return showChangedOnly;
    }

    public boolean toggleShowChangedOnly() {
        return showChangedOnly = !showChangedOnly;
    }

    public void prepare() {
        long timestamp = System.currentTimeMillis();

        if (loadMap.size() > 0 && loadMapStart == null) {
            loadMapStart = loadMap;
        }
        loadMapPrev = loadMap;
        loadMap = new TreeMap<>();

        if (summary.size() > 0 && summaryStart == null) {
            summaryStart = summary;
        }
        summaryPrev = summary;
        summary = new LoadRecord();

        Util.printVerboseMessage(args, "Load.prepare", timestamp);
    }

    public Map<Level, LoadRecord> getLoadMap() {
        return loadMap;
    }

    @VisibleForTesting
    public void setLoadMap(Map<Level, LoadRecord> loadMap) {
        this.loadMap = loadMap;
    }

    void setLoadMapStart(Map<Level, LoadRecord> loadMapStart, long timestampStart) {
        this.loadMapStart = loadMapStart;
        this.timestampStart = timestampStart;
    }

    public Map<Level, LoadRecord> getLoadMapPrev() {
        if (diffFromStart) {
            return loadMapStart;
        } else {
            return loadMapPrev;
        }
    }

    public LoadRecord getSummary() {
        return summary;
    }

    @VisibleForTesting
    public void setSummary(LoadRecord summary) {
        this.summary = summary;
    }

    public LoadRecord getSummaryPrev() {
        if (diffFromStart) {
            return summaryStart;
        } else {
            return summaryPrev;
        }
    }

    private void summary(LoadEntry loadEntry, Number value) {
        Number prev = getSummary().get(loadEntry);
        getSummary().put(loadEntry, loadEntry.add(prev, value));
    }

    void update(TableInfo tableInfo, Args args) {
        long timestamp = System.currentTimeMillis();

        if (tableInfo != null) {
            for (RegionInfo hRegionInfo : tableInfo.getRegionInfoSet()) {
                if (args.has(Args.OPTION_TEST)
                        && !CommandAdapter.getTableName(hRegionInfo).getNameAsString().startsWith(Constant.UNIT_TEST_TABLE_PREFIX))
                    continue;

                if (tableInfo.getServerIndexes(args).size() > 0) {
                    int serverIndex = tableInfo.serverIndex(hRegionInfo);
                    if (!tableInfo.getServerIndexes(args).contains(serverIndex)) continue;
                }

                final Level level = levelClass.createLevel(hRegionInfo, tableInfo);
                LoadRecord loadRecord = loadMap.get(level);

                if (loadRecord == null) {
                    loadRecord = new LoadRecord();
                    loadMap.put(level, loadRecord);
                }

                for (LoadEntry loadEntry : RegionLoadAdapter.loadEntries) {
                    RegionLoadDelegator regionLoad = tableInfo.getRegionLoad(hRegionInfo);
                    Number valueCur = regionLoad == null ? 0 : loadEntry.getValue(regionLoad);
                    Number valuePrev = loadRecord.get(loadEntry) == null ? 0 : loadRecord.get(loadEntry);
                    loadRecord.put(loadEntry, loadEntry.add(valueCur, valuePrev));

                    summary(loadEntry, valueCur);
                }
            }

            updateChangeMap();
        }

        // reset timestamp after update
        resetTimestamp();

        loadIO.saveOutput(args);

        Util.printVerboseMessage(args, "Load.update", timestamp);
    }

    @VisibleForTesting
    public void updateChangeMap() {
        if (getLoadMapPrev() != null && getLoadMapPrev().size() > 0) {
            clearChangeMap();

            for (Map.Entry<Level, LoadRecord> mapEntry : getLoadMap().entrySet()) {
                Level level = mapEntry.getKey();

                for (Map.Entry<LoadEntry, Number> load : mapEntry.getValue().entrySet()) {
                    LoadEntry loadEntry = load.getKey();
                    LoadRecord loadRecordPrev = getLoadMapPrev().get(level);
                    Number valuePrev = loadRecordPrev == null ? null : loadRecordPrev.get(loadEntry);
                    Number valueCur = getLoadMap().get(level).get(loadEntry);
                    boolean notChanged = loadEntry.equals(valuePrev, valueCur);
                    if (!notChanged) {
                        recordChangeMap.put(level, true);
                        valueChangeMap.get(level).put(loadEntry, ChangeState.changed.ordinal());
                        summaryChangeMap.put(loadEntry, ChangeState.changed.ordinal());
                    }
                }
            }
        }
    }

    private void clearChangeMap() {
        recordChangeMap.clear();
        for (Level level : getLoadMap().keySet()) {
            recordChangeMap.put(level, false);
        }

        valueChangeMap.clear();
        for (Map.Entry<Level, LoadRecord> loadRecordEntry : getLoadMap().entrySet()) {
            Level level = loadRecordEntry.getKey();
            for (Map.Entry<LoadEntry, Number> load : loadRecordEntry.getValue().entrySet()) {
                LoadEntry loadEntry = load.getKey();
                LoadRecord loadRecord = valueChangeMap.get(level);
                if (loadRecord == null) {
                    loadRecord = new LoadRecord();
                    valueChangeMap.put(level, loadRecord);
                }
                loadRecord.put(loadEntry, ChangeState.not_changed.ordinal());
            }
        }

        summaryChangeMap = new LoadRecord();
    }

    public List<Level> sortedLevels() {
        if (sortKey == null || sortKey.equals(SortKey.DEFAULT)) {
            return new ArrayList<>(loadMap.keySet());
        } else {
            return sortKey.sortedLevels(this);
        }
    }

    public SortKey getSortKey() {
        return sortKey;
    }

    public void setSortKey(SortKey sortKey) {
        if (sortKey == null) throw new IllegalArgumentException("sortKey should not be null");

        this.sortKey = sortKey;
    }

    public String getSortKeyInfo() {
        if (sortKey.equals(SortKey.DEFAULT)) {
            return levelClass.getLevelTypeString();
        } else {
            return sortKey.toString();
        }
    }

    Number getValueDiff(Level level, LoadEntry loadEntry) {
        Map<Level, LoadRecord> loadMapPrev = getLoadMapPrev();
        LoadRecord loadRecordPrev = loadMapPrev == null ? null : loadMapPrev.get(level);
        if (loadRecordPrev == null) {
            return null;
        } else {
            Number valuePrev = loadRecordPrev.get(loadEntry);
            Number valueCur = getLoadMap().get(level).get(loadEntry);
            return loadEntry.diff(valueCur, valuePrev);
        }
    }

    public String getValueDiffString(Level level, LoadEntry loadEntry) {
        Number valueDiff = getValueDiff(level, loadEntry);
        if (valueDiff == null) {
            return LoadEntry.NOT_AVAILABLE;
        } else {
            if (isShowRate()) {
                return loadEntry.toRateString(valueDiff, getDuration());
            } else {
                return loadEntry.toString(valueDiff);
            }
        }
    }

    public String getSummaryDiffString(LoadEntry loadEntry) {
        LoadRecord summaryPrev = getSummaryPrev();
        Number valuePrev = summaryPrev == null ? null : summaryPrev.get(loadEntry);
        if (valuePrev == null) {
            return LoadEntry.NOT_AVAILABLE;
        } else {
            Number valueCur = getSummary().get(loadEntry);
            Number valueDiff = loadEntry.diff(valueCur, valuePrev);
            if (isShowRate()) {
                return loadEntry.toRateString(valueDiff, getDuration());
            } else {
                return loadEntry.toString(valueDiff);
            }
        }
    }

    public String save(Args args) {
        return loadIO.save(args);
    }

    public void load(Args args, String input) {
        loadIO.load(args, input);
    }

    public String showFiles(Args args) {
        return loadIO.showSavedFiles(args);
    }

    private enum ChangeState {changed, not_changed}
}
