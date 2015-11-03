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
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Wrapper class for the secured cluster to list tables without the privileges CREATE or ADMIN.
 */
public class HBaseAdminWrapper extends HBaseAdmin {
    @SuppressWarnings("deprecation")
    public HBaseAdminWrapper(Configuration conf) throws IOException {
        super(conf);
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
        return listTables((String) null);
    }

    @Override
    public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public TableName[] listTableNames() throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public TableName[] listTableNamesByNamespace(String name) throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        throw new IllegalStateException("Not implemented yet.");
    }

    @Override
    public HTableDescriptor[] listTables(String regex) throws IOException {
        Set<TableName> tableNameSet = new TreeSet<>();
        for (HRegionInfo hRegionInfo : MetaScanner.listAllRegions(getConfiguration(), getConnection(), true)) {
            TableName tableName = hRegionInfo.getTable();
            if (!tableName.getNameAsString().startsWith("hbase:")) {
                if (regex == null) {
                    tableNameSet.add(tableName);
                } else {
                    if (tableName.getNameAsString().matches(regex)) tableNameSet.add(tableName);
                }
            }
        }

        HTableDescriptor[] hTableDescriptors = new HTableDescriptor[tableNameSet.size()];
        int i = 0;
        for (TableName tableName : tableNameSet) {
            hTableDescriptors[i++] = new HTableDescriptor(tableName);
        }
        return hTableDescriptors;
    }

    @SuppressWarnings("deprecation")
    @Override
    public String[] getTableNames() throws IOException {
        throw new IllegalStateException("Deprecated. Do not use this.");
    }

    @SuppressWarnings("deprecation")
    @Override
    public String[] getTableNames(Pattern pattern) throws IOException {
        throw new IllegalStateException("Deprecated. Do not use this.");
    }

    @SuppressWarnings("deprecation")
    @Override
    public String[] getTableNames(String regex) throws IOException {
        throw new IllegalStateException("Deprecated. Do not use this.");
    }
}
