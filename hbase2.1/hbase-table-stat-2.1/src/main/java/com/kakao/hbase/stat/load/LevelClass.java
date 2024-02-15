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

import com.kakao.hbase.common.Args;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;

public class LevelClass {
    private final Class levelClass;

    public LevelClass(Class levelClass) {
        this.levelClass = levelClass;
    }

    LevelClass(boolean multiTable, Args args) {
        if (multiTable && !args.has(Args.OPTION_REGION_SERVER) && !args.has(Args.OPTION_REGION)) {
            this.levelClass = TableName.class;
        } else if (args.has(Args.OPTION_REGION)) {
            this.levelClass = RegionName.class;
        } else {
            this.levelClass = ServerName.class;
        }
    }

    public String getLevelTypeString() {
        if (levelClass == TableName.class) {
            return "Table";
        } else if (levelClass == RegionName.class) {
            return "Region (RS Index)";
        } else if (levelClass == ServerName.class) {
            return "RegionServer";
        } else {
            return levelClass.getSimpleName();
        }
    }

    Level createLevel(RegionInfo hRegionInfo, TableInfo tableInfo) {
        if (levelClass == TableName.class) {
            return new Level(hRegionInfo.getTable());
        } else if (levelClass == RegionName.class) {
            return new Level(new RegionName(hRegionInfo, tableInfo.serverIndex(hRegionInfo)));
        } else {
            return new Level(tableInfo.getServer(hRegionInfo));
        }
    }

    public Class getLevelClass() {
        return levelClass;
    }
}
