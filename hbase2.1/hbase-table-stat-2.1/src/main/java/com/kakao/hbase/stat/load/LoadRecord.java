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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class LoadRecord {
    private final Map<LoadEntry, Number> map = new TreeMap<>();

    public Number get(LoadEntry loadEntry) {
        return map.get(loadEntry);
    }

    public void put(LoadEntry loadEntry, Number number) {
        map.put(loadEntry, number);
    }

    public Set<Map.Entry<LoadEntry, Number>> entrySet() {
        return map.entrySet();
    }

    public int size() {
        return map.size();
    }
}
