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
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import com.kakao.hbase.specific.RegionLoadAdapter;
import com.kakao.hbase.specific.RegionLoadDelegator;
import com.kakao.hbase.specific.RegionLocationCleaner;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TableInfo {
    private final HBaseAdmin admin;
    private final Load load;
    private final String tableName;
    private final Args args;
    private NavigableMap<HRegionInfo, ServerName> regionServerMap;
    private Map<byte[], HRegionInfo> regionMap;
    private Set<ServerName> serverNameSet = new TreeSet<>();
    private RegionLoadAdapter regionLoadAdapter;
    private Set<Integer> indexRSs = null;

    public TableInfo(HBaseAdmin admin, String tableName, Args args) throws Exception {
        super();

        this.admin = admin;
        this.tableName = tableName;
        this.args = args;

        load = new Load(new LevelClass(isMultiTable(), args));
    }

    public String getTableName() {
        return tableName;
    }

    private boolean isMultiTable() throws IOException {
        try {
            return tableName.equals(Args.ALL_TABLES) || !admin.tableExists(tableName);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Illegal character code")
                || e.getMessage().contains("Illegal first character")
                || e.getMessage().contains("Namespaces can only start with alphanumeric characters"))
                return true;
            throw e;
        }
    }

    public Load getLoad() {
        return load;
    }

    public Set<HRegionInfo> getRegionInfoSet() {
        Set<HRegionInfo> regionInfoSet = new TreeSet<>();
        for (Map.Entry<HRegionInfo, ServerName> entry : regionServerMap.entrySet()) {
            regionInfoSet.add(entry.getKey());
        }
        return regionInfoSet;
    }

    public RegionLoadDelegator getRegionLoad(HRegionInfo hRegionInfo) {
        return regionLoadAdapter.get(hRegionInfo);
    }

    public ServerName getServer(HRegionInfo regionInfo) {
        return regionServerMap.get(regionInfo);
    }

    private void prepare() throws Exception {
        long timestamp = System.currentTimeMillis();

        load.prepare();

        initializeRegionServerMap();
        initializeRegionBytesMap();
        regionLoadAdapter = new RegionLoadAdapter(admin, regionMap, args);

        Util.printVerboseMessage(args, "TableInfo.prepare", timestamp);
    }

    private void initializeRegionServerMap() throws Exception {
        long timestamp = System.currentTimeMillis();

        if (load.getLevelClass().getLevelClass() == RegionName.class || args.has(Args.OPTION_REGION_SERVER)) {
            initializeServerNameSet();
        }

        Set<String> tables = Args.tables(args, admin);
        if (tables == null) {
            regionServerMap = CommandAdapter.regionServerMap(args, admin.getConfiguration()
                , admin.getConnection(), false);
        } else {
            regionServerMap = CommandAdapter.regionServerMap(args, admin.getConfiguration()
                , admin.getConnection(), tables, false);
        }
        clean(regionServerMap);

        Util.printVerboseMessage(args, "TableInfo.initializeRegionServerMap", timestamp);
    }

    private void clean(NavigableMap<HRegionInfo, ServerName> regionServerMap) throws InterruptedException, IOException {
        long timestamp = System.currentTimeMillis();

        Set<String> tableNameSet = tableNameSet(regionServerMap);

        ExecutorService executorService = Executors.newFixedThreadPool(RegionLocationCleaner.THREAD_POOL_SIZE);
        for (String tableName : tableNameSet)
            executorService.execute(new RegionLocationCleaner(tableName, admin.getConfiguration(), regionServerMap));
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        Util.printVerboseMessage(args, "TableInfo.clean", timestamp);
    }

    private Set<String> tableNameSet(NavigableMap<HRegionInfo, ServerName> regionServerMap) {
        long timestamp = System.currentTimeMillis();

        Set<String> tableNameSet = new TreeSet<>();
        for (Map.Entry<HRegionInfo, ServerName> entry : regionServerMap.entrySet()) {
            tableNameSet.add(CommandAdapter.getTableName(entry.getKey()));
        }

        Util.printVerboseMessage(args, "TableInfo.tableNameSet", timestamp);
        return tableNameSet;
    }

    private void initializeServerNameSet() throws IOException {
        long timestamp = System.currentTimeMillis();

        serverNameSet = new TreeSet<>(admin.getClusterStatus().getServers());

        Util.printVerboseMessage(args, "TableInfo.initializeServerNameSet", timestamp);
    }

    /**
     * For looking up HRegionInfo instances by byte[]
     */
    private void initializeRegionBytesMap() {
        long timestamp = System.currentTimeMillis();

        regionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

        for (Map.Entry<HRegionInfo, ServerName> entry : regionServerMap.entrySet()) {
            regionMap.put(entry.getKey().getRegionName(), entry.getKey());
        }

        Util.printVerboseMessage(args, "TableInfo.initializeRegionBytesMap", timestamp);
    }

    /**
     * Refresh region load information by querying data from HBase cluster.
     *
     * @throws Exception
     */
    public void refresh() throws Exception {
        if (load.isUpdating()) return;

        synchronized (load) {
            load.setIsUpdating(true);

            long timestamp = System.currentTimeMillis();

            prepare();

            load.update(this, args);

            Util.printVerboseMessage(args, "TableInfo.refresh", timestamp);

            load.setIsUpdating(false);
        }
    }

    /**
     * Return zero based index of a region server that serves the given region.
     *
     * @param hRegionInfo region
     * @return region server index from all region servers
     */
    public int serverIndex(HRegionInfo hRegionInfo) {
        ServerName serverName = regionServerMap.get(hRegionInfo);
        return Arrays.asList(serverNameSet.toArray()).indexOf(serverName);
    }

    Set<Integer> getServerIndexes(Args args) {
        if (indexRSs == null) {
            indexRSs = new HashSet<>();
            if (args.has(Args.OPTION_REGION_SERVER)) {
                Object arg = args.valueOf(Args.OPTION_REGION_SERVER);
                if (arg != null) {
                    int i = 0;
                    for (ServerName serverName : serverNameSet) {
                        if (serverName.getServerName().matches((String) arg)) {
                            indexRSs.add(i);
                        }
                        i++;
                    }

                    if (indexRSs.size() == 0)
                        throw new IllegalStateException(arg + " is invalid");
                }
            }
        }

        return indexRSs;
    }
}
