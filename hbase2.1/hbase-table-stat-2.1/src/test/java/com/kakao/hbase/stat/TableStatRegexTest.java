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

package com.kakao.hbase.stat;

import com.kakao.hbase.stat.load.Level;
import org.apache.hadoop.hbase.TableName;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TableStatRegexTest extends StatTestBase {
    public TableStatRegexTest() {
        super(TableStatRegexTest.class);
    }

    @Test
    public void testRegexTableNameWithRS() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0", "--rs"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("RegionServer", command.getLoad().getLevelClass().getLevelTypeString());
    }

    @Test
    public void testRegexTableNameWithRegion() throws Exception {
        String tableNameRegex = tableName + ".*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0", "--region"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("Region (RS Index)", command.getLoad().getLevelClass().getLevelTypeString());
    }

    @Test
    public void testRegexTableName() throws Exception {
        TableName tableName2 = createAdditionalTable(tableName + "2");
        TableName tableName3 = createAdditionalTable(tableName + "22");

        String tableNameRegex = tableName + "2.*";
        String[] args = {"zookeeper", tableNameRegex, "--interval=0"};
        TableStat command = new TableStat(admin, new StatArgs(args));

        command.run();
        Assert.assertEquals("Table", command.getLoad().getLevelClass().getLevelTypeString());
        Assert.assertEquals(2, command.getLoad().getLoadMap().size());
        Set<Level> levelSet = command.getLoad().getLoadMap().keySet();
        Level[] levels = levelSet.toArray(new Level[command.getLoad().getLoadMap().size()]);
        assertEquals(tableName2.getNameAsString(), levels[0].toString());
        assertEquals(tableName3.getNameAsString(), levels[1].toString());
        Assert.assertEquals(0, command.getLoad().getLoadMapPrev().size());
    }
}
