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

package com.kakao.hbase.snapshot;

import com.google.common.annotations.VisibleForTesting;
import com.kakao.hbase.SnapshotArgs;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.HBaseClient;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.SnapshotAdapter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;

public class Snapshot implements Watcher {
    static final SimpleDateFormat DATE_FORMAT_SNAPSHOT = new SimpleDateFormat("yyyyMMddHHmmss");
    static final int ABORT_ZNODE_AGE_THRESHOLD_MS = 24 * 60 * 60 * 1000;
    static final String TIMESTAMP_PREFIX = "_S";
    private static final int SESSION_TIMEOUT = 120000;
    private static final SimpleDateFormat DATE_FORMAT_LOG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String ABORT_WATCH_PREFIX = "/hbase/online-snapshot/abort/";
    private static final String ACQUIRED_WATCH_PREFIX = "/hbase/online-snapshot/acquired/";
    private static final int MAX_RETRY = 10;
    private static final long RETRY_INTERVAL = 2000;
    @VisibleForTesting
    static boolean skipCheckTableExistence = false;
    private final SnapshotArgs args;
    private final Connection connection;

    // for testing
    private final Map<TableName, Integer> tableSnapshotCountMaxMap = new HashMap<>();

    Snapshot(Connection connection, SnapshotArgs args) {
        this.connection = connection;
        this.args = args;
    }

    private static void setLoggingThreshold(String loggingLevel) {
        Properties props = new Properties();
        props.setProperty("log4j.threshold", loggingLevel);
        PropertyConfigurator.configure(props);
    }

    private static String usage() {
        return "Create snapshots of HBase tables.\n"
                + "usage: " + Snapshot.class.getSimpleName().toLowerCase()
                + " (<zookeeper quorum>|<args file>) <tables expression>\n"
                + "  options:\n"
                + "    --" + SnapshotArgs.OPTION_KEEP
                + "=<num of snapshots> : The number of snapshots to keep. Default 0(unlimited).\n"
                + "    --" + SnapshotArgs.OPTION_SKIP_FLUSH
                + "=<tables expression> : Tables to skip flush. Default false.\n"
                + "    --" + SnapshotArgs.OPTION_OVERRIDE
                + "=<tables in list type> : List to override.\n"
                + "    --" + SnapshotArgs.OPTION_EXCLUDE
                + "=<tables expression> : Tables to be excluded.\n"
                + "    --" + SnapshotArgs.OPTION_CLEAR_WATCH_LEAK
                + " : Clear watch leaks. Workaround for HBASE-13885. This is not necessary as of HBase 0.98.14.\n"
                + "    --" + SnapshotArgs.OPTION_DELETE_SNAPSHOT_FOR_NOT_EXISTING_TABLE
                + " : Delete the snapshots for not existing tables.\n"
                + Args.commonUsage()
                + "  tables expression:\n"
                + "    regexp or comma separated list\n"
                + "  list entity format:\n"
                + "    <table name>[/<keep count>/<skip flush>]\n"
                + "    regexp\n";
    }

    public static void main(String[] argsParam) throws Exception {
        setLoggingThreshold("ERROR");

        SnapshotArgs args;
        try {
            args = new SnapshotArgs(argsParam);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.out.println();
            System.out.println(usage());
            System.exit(1);
            throw e;
        }

        Connection connection = HBaseClient.getConnection(args);

        Snapshot app = new Snapshot(connection, args);
        app.run();
    }

    @VisibleForTesting
    int getMaxCount(TableName tableName) {
        return tableSnapshotCountMaxMap.get(tableName);
    }

    void run() throws IOException, KeeperException, InterruptedException {
        try (Admin admin = connection.getAdmin()) {
            String timestamp = timestamp(TimestampFormat.snapshot);

            String connectString = admin.getConfiguration().get("hbase.zookeeper.quorum");
            ZooKeeper zooKeeper = null;
            try {
                // snapshot name, table
                Map<String, TableName> failedSnapshotMap = new TreeMap<>();

                zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
                for (TableName tableName : args.tableSet(admin)) {
                    if (args.isExcluded(tableName)) {
                        System.out.println(timestamp(TimestampFormat.log)
                                + " - Table \"" + tableName + "\" - EXCLUDED");
                        continue;
                    }

                    String snapshotName = getPrefix(tableName) + timestamp;
                    try {
                        snapshot(zooKeeper, tableName, snapshotName);
                    } catch (Throwable e) {
                        failedSnapshotMap.put(snapshotName, tableName);
                    }

                    // delete old snapshots after creating new one
                    deleteOldSnapshots(admin, tableName);
                }
                deleteSnapshotsForNotExistingTables();
                deleteOldAbortZnodes(zooKeeper);

                retrySnapshot(zooKeeper, failedSnapshotMap);
                Util.sendAlertAfterSuccess(args, this.getClass());
            } catch (Throwable e) {
                String message = "ConnectionString= " + connectString + ", CurrentHost= "
                        + InetAddress.getLocalHost().getHostName() + ", Message= " + errorMessage(e);
                System.out.println("\n" + timestamp(TimestampFormat.log) + " - " + message);
                Util.sendAlertAfterFailed(args, this.getClass(), message);
                throw e;
            } finally {
                if (zooKeeper != null && zooKeeper.getState().isConnected()) {
                    zooKeeper.close();
                }
            }
        }
    }

    private void deleteOldAbortZnodes(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        String parentZnode = ABORT_WATCH_PREFIX.substring(0, ABORT_WATCH_PREFIX.length() - 1);
        List<String> children = zooKeeper.getChildren(parentZnode, false);
        for (String snapshotName : children) {
            if (SnapshotUtil.isOldZnode(snapshotName)) {
                String abortZnode = ABORT_WATCH_PREFIX + snapshotName;
                System.out.println(timestamp(TimestampFormat.log) + " - znode deleted - " + abortZnode);
                zooKeeper.delete(abortZnode, -1);
            }
        }
    }

    /**
     * Retry failed snapshots
     */
    private void retrySnapshot(ZooKeeper zooKeeper, Map<String, TableName> failedSnapshotMap)
            throws IOException, KeeperException, InterruptedException {
        try (Admin admin = connection.getAdmin()) {
            if (!failedSnapshotMap.isEmpty()) {
                System.out.println();
                Util.printMessage("--------------------- Retrying failed snapshots -----------------------------");
                for (Map.Entry<String, TableName> entry : failedSnapshotMap.entrySet()) {
                    Util.printMessage("Table: " + entry.getValue() + ", Snapshot: " + entry.getKey());
                }

                for (Map.Entry<String, TableName> entry : failedSnapshotMap.entrySet()) {
                    String snapshotName = entry.getKey();
                    TableName tableName = entry.getValue();
                    if (exists(admin, snapshotName) || skipCheckTableExistence) {
                        snapshot(zooKeeper, tableName, snapshotName);
                    } else {
                        Util.printMessage("Table does not exist - " + tableName + " - SKIPPED");
                    }
                }
                deleteSnapshotsForNotExistingTables();
            }
        }
    }

    private void deleteSnapshotsForNotExistingTables() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (args.has(Args.OPTION_DELETE_SNAPSHOT_FOR_NOT_EXISTING_TABLE)) {
                List<SnapshotDescription> snapshots = admin.listSnapshots();
                for (SnapshotDescription snapshot : snapshots) {
                    TableName tableName = snapshot.getTableName();
                    String snapshotName = snapshot.getName();
                    if (snapshotName.startsWith(getPrefix(tableName))) {
                        if (!admin.tableExists(tableName)) {
                            System.out.print(timestamp(TimestampFormat.log) + " - Table \"" + tableName
                                    + "\" - Delete snapshot - Not existing table - \"" + snapshotName + "\"");
                            admin.deleteSnapshot(snapshotName);
                            System.out.println(" - OK");
                        }
                    } else {
                        System.out.println(timestamp(TimestampFormat.log) + " - Table \"" + tableName
                                + "\" - Delete snapshot - \"" + snapshotName + "\" - SKIPPED");
                    }
                }
            }
        }
    }

    private String errorMessage(Throwable e) {
        return "Snapshot Failed - " + e.getMessage();
    }

    String getPrefix(TableName tableName) {
        return tableName.getNameAsString().replace(":", "_") + TIMESTAMP_PREFIX;
    }

    @VisibleForTesting
    void snapshot(ZooKeeper zooKeeper, TableName tableName, String snapshotName)
            throws IOException, KeeperException, InterruptedException {
        try (Admin admin = connection.getAdmin()) {
            if (args.has(Args.OPTION_TEST) && !tableName.getNameAsString().startsWith("UNIT_TEST_")) return;

            System.out.print(timestamp(TimestampFormat.log) + " - Table \"" + tableName
                    + "\" - Create Snapshot - \"" + snapshotName + "\" - ");
            if (!exists(admin, snapshotName)) {
                for (int i = 1; i <= MAX_RETRY; i++) {
                    try {
                        if (!exists(admin, snapshotName)) {
                            admin.snapshot(snapshotName, tableName, args.flushType(tableName));
                        }
                        break;
                    } catch (IOException e) {
                        if (i == MAX_RETRY) {
                            throw new IllegalStateException("Snapshot failed.");
                        }
                        if (e.getMessage().contains("org.apache.zookeeper.KeeperException$NoNodeException")) {
                            // delete dubious snapshot
                            if (exists(admin, snapshotName)) {
                                admin.deleteSnapshot(snapshotName);
                                System.out.println(timestamp(TimestampFormat.log)
                                        + " - Delete dubious snapshot " + snapshotName);
                            }

                            System.out.println(timestamp(TimestampFormat.log)
                                    + " - RETRY(" + i + "/" + MAX_RETRY + ") - " + e.getMessage());
                            Thread.sleep(RETRY_INTERVAL);
                        } else {
                            throw e;
                        }
                    }
                }

                if (args.has(Args.OPTION_CLEAR_WATCH_LEAK))
                    clearAbortWatchLeak(zooKeeper, snapshotName);

                System.out.println("OK");
            } else {
                System.out.println("SKIPPED");
            }
        } catch (Throwable e) {
            System.out.println("FAILED");
            throw e;
        }
    }

    private void clearAbortWatchLeak(ZooKeeper zooKeeper, String snapshotName)
            throws KeeperException, InterruptedException {
        if (zooKeeper == null || !zooKeeper.getState().isConnected()) return;
        createEmptyEphemeralZnode(zooKeeper, ABORT_WATCH_PREFIX + snapshotName);
        createEmptyEphemeralZnode(zooKeeper, ACQUIRED_WATCH_PREFIX + snapshotName);
    }

    private void createEmptyEphemeralZnode(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) != null) return;
        zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private String timestamp(TimestampFormat format) {
        if (format == TimestampFormat.log) {
            return DATE_FORMAT_LOG.format(System.currentTimeMillis());
        } else {
            return DATE_FORMAT_SNAPSHOT.format(System.currentTimeMillis());
        }
    }

    private void deleteOldSnapshots(Admin admin, TableName tableName) throws IOException {
        if (args.keepCount(tableName) == SnapshotArgs.KEEP_UNLIMITED
                || args.has(Args.OPTION_TEST) && !tableName.getNameAsString().startsWith("UNIT_TEST_")) {
            System.out.println(timestamp(TimestampFormat.log)
                    + " - Table \"" + tableName + "\" - Delete Snapshot - Keep Unlimited - SKIPPED");
            return;
        }

        List<SnapshotDescription> sd = SnapshotAdapter.getSnapshotDescriptions(admin, getPrefix(tableName) + ".*");
        int snapshotCounter = sd.size();
        tableSnapshotCountMaxMap.put(tableName, snapshotCounter);
        for (SnapshotDescription d : sd) {
            if (snapshotCounter-- > args.keepCount(tableName)) {
                String snapshotName = d.getName();
                System.out.print(timestamp(TimestampFormat.log)
                        + " - Table \"" + tableName + "\" - Delete Snapshot - Keep "
                        + args.keepCount(tableName) + " - \"" + snapshotName + "\" - ");
                admin.deleteSnapshot(snapshotName);
                System.out.println("OK");
            }
        }
    }

    private boolean exists(Admin admin, String targetSnapshotName) throws IOException {
        List<SnapshotDescription> sd = SnapshotAdapter.getSnapshotDescriptions(admin, targetSnapshotName);
        return sd.size() > 0;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }

    private enum TimestampFormat {log, snapshot}
}
