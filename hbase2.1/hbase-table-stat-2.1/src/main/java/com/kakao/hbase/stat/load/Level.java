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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;

public class Level implements Comparable<Level> {
    private final Object level;

    public Level(Object level) {
        this.level = level;
    }

    @Override
    public String toString() {
        if (level instanceof RegionName) {
            return ((RegionName) level).name();
        } else if (level instanceof ServerName) {
            return ((ServerName) level).getServerName();
        } else if (level instanceof TableName) {
            return ((TableName) level).getNameAsString();
        } else {
            return level.toString();
        }
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(Level thatLevel) {
        if (thatLevel == null) {
            return 1;
        }
        if (level instanceof RegionName) {
            return ((RegionName) level).compareTo((RegionName) thatLevel.level);
        } else if (level instanceof ServerName) {
            return ((ServerName) level).compareTo((ServerName) thatLevel.level);
        } else if (level instanceof TableName) {
            return ((TableName) level).compareTo((TableName) thatLevel.level);
        } else if (level instanceof String) {
            return ((String) level).compareTo((String) thatLevel.level);
        } else
            throw new RuntimeException("can not compareTo");
    }

    public boolean equalsName(String name) {
        if (name == null) return false;

        if (level instanceof RegionName) {
            return ((RegionName) level).name().equals(name);
        } else if (level instanceof ServerName) {
            return ((ServerName) level).getServerName().equals(name);
        } else if (level instanceof TableName) {
            return ((TableName) level).getNameAsString().equals(name);
        } else if (level instanceof String) {
            return level.equals(name);
        } else
            throw new RuntimeException("can not equalsName");
    }
}
