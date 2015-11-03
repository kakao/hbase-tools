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

/**
 * For HBase 1.0
 */
public class RegionLoadDelegator {
    private final org.apache.hadoop.hbase.RegionLoad regionLoad;

    public RegionLoadDelegator(org.apache.hadoop.hbase.RegionLoad regionLoad) {
        this.regionLoad = regionLoad;
    }

    public static LoadEntry[] loadEntries() {
        return LoadEntry.values();
    }

    public int getStorefiles() {
        return regionLoad.getStorefiles();
    }

    public int getStoreUncompressedSizeMB() {
        return regionLoad.getStoreUncompressedSizeMB();
    }

    public int getStorefileSizeMB() {
        return regionLoad.getStorefileSizeMB();
    }

    public long getReadRequestsCount() {
        return regionLoad.getReadRequestsCount();
    }

    public long getWriteRequestsCount() {
        return regionLoad.getWriteRequestsCount();
    }

    public int getMemStoreSizeMB() {
        return regionLoad.getMemStoreSizeMB();
    }

    public long getCurrentCompactedKVs() {
        return regionLoad.getCurrentCompactedKVs();
    }

    public int getRegions() {
        return 1;
    }

    public float getDataLocality() {
        return regionLoad.getDataLocality();
    }
}