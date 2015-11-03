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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Wrapper class for the secured cluster to list tables without the privileges CREATE or ADMIN.
 */
public class HBaseAdminWrapper extends HBaseAdmin {
    public HBaseAdminWrapper(Configuration conf) throws IOException {
        super(conf);
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
        return listTables((String) null);
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public HTableDescriptor[] listTables(String regex) throws IOException {
        Set<String> tableNameSet = new TreeSet<>();
        for (HRegionInfo hRegionInfo : MetaScanner.listAllRegions(getConfiguration(), true)) {
            String tableName = hRegionInfo.getTableNameAsString();
            if (!tableName.startsWith("hbase:")) {
                if (regex == null) {
                    tableNameSet.add(tableName);
                } else {
                    if (tableName.matches(regex)) tableNameSet.add(tableName);
                }
            }
        }

        HTableDescriptor[] hTableDescriptors = new HTableDescriptor[tableNameSet.size()];
        int i = 0;
        for (String tableName : tableNameSet) {
            hTableDescriptors[i++] = new HTableDescriptor(tableName);
        }
        return hTableDescriptors;
    }

    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public HTableDescriptor[] getTableDescriptors(List<String> tableNames) throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }
}
