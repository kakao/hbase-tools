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

import java.util.Map;

class Length {
    private int value;
    private int diff;
    private int total;

    private Length() {
        this.value = 0;
        this.diff = 0;
        this.total = 0;
    }

    static Length getLength(Map<String, Length> lengthMap, String key) {
        Length length = lengthMap.get(key);
        if (length == null) {
            length = new Length();
            lengthMap.put(key, length);
        }
        return length;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    int getDiff() {
        return diff;
    }

    void setDiff(int diff) {
        this.diff = diff;
    }

    int getTotal() {
        return total;
    }

    void setTotal(int total) {
        this.total = total;
    }
}
