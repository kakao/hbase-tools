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

package com.kakao.hbase.specific;

import com.kakao.hbase.common.LoadEntry;
import org.apache.hadoop.hbase.Size;

/**
 * For HBase 2.1
 */
public class RegionLoadDelegator {
    private final org.apache.hadoop.hbase.RegionMetrics regionMetrics;

    RegionLoadDelegator(org.apache.hadoop.hbase.RegionMetrics regionMetrics) {
        this.regionMetrics = regionMetrics;
    }

    public int getStoreFileCount() {
        return regionMetrics.getStoreFileCount();
    }

    public Size getUncompressedStoreFileSize() {
        return regionMetrics.getUncompressedStoreFileSize();
    }

    public int getStoreFileSizeMB() {
        return (int) regionMetrics.getStoreFileSize().get(Size.Unit.MEGABYTE);
    }

    public long getReadRequestCount() {
        return regionMetrics.getReadRequestCount();
    }

    public long getWriteRequestCount() {
        return regionMetrics.getWriteRequestCount();
    }

    public int getMemStoreSizeMB() {
        return (int) regionMetrics.getMemStoreSize().get(Size.Unit.MEGABYTE);
    }

    public long getCompactedCellCount() {
        return regionMetrics.getCompactedCellCount();
    }

    public int getRegions() {
        return 1;
    }

    public float getDataLocality() {
        return regionMetrics.getDataLocality();
    }

    static LoadEntry[] loadEntries() {
        final LoadEntry[] loadEntries = new LoadEntry[LoadEntry.values().length - 1];
        int i = 0;
        for (LoadEntry loadEntry : LoadEntry.values()) {
            if (loadEntry != LoadEntry.DataLocality) {
                loadEntries[i++] = loadEntry;
            }
        }
        return loadEntries;
    }
}
