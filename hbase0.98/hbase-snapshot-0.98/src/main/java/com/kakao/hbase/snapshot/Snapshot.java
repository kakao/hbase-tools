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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;

public class Snapshot implements Watcher {
    private static final int SESSION_TIMEOUT = 120000;
    private static final SimpleDateFormat DATE_FORMAT_SNAPSHOT = new SimpleDateFormat("yyyyMMddHHmmss");
    private static final SimpleDateFormat DATE_FORMAT_LOG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String ABORT_WATCH_PREFIX = "/hbase/online-snapshot/abort/";
    private static final String TIMESTAMP_PREFIX = "_S";
    private static final int MAX_RETRY = 10;
    private static final long RETRY_INTERVAL = 2000;
    private final SnapshotArgs args;
    private final HBaseAdmin admin;

    // for testing
    private final Map<String, Integer> tableSnapshotCountMaxMap = new HashMap<>();

    public Snapshot(HBaseAdmin admin, SnapshotArgs args) {
        this.admin = admin;
        this.args = args;
    }

    static void setLoggingThreshold(String loggingLevel) {
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
                + "    --" + SnapshotArgs.OPTION_CLEAR_WATCH_LEAK_ONLY
                + " : Clear watch leaks only. It does not create any snapshot. Workaround for HBASE-13885."
                + " This is not necessary as of HBase 0.98.14.\n"
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

        HBaseAdmin admin = HBaseClient.getAdmin(args);

        Snapshot app = new Snapshot(admin, args);
        app.run();
    }

    @VisibleForTesting
    int getMaxCount(String tableName) {
        return tableSnapshotCountMaxMap.get(tableName);
    }

    public void run() throws IOException, KeeperException, InterruptedException {
        String timestamp = timestamp(TimestampFormat.snapshot);

        String connectString = admin.getConfiguration().get("hbase.zookeeper.quorum");
        ZooKeeper zooKeeper = null;
        try {
            Map<String, String> failedSnapshotMap = new TreeMap<>();

            zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
            if (args.has(Args.OPTION_CLEAR_WATCH_LEAK_ONLY)) {
                clearWatchLeak(args, zooKeeper);
            } else {
                for (String tableName : args.tableSet(admin)) {
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

                // retry failed snapshots
                if (!failedSnapshotMap.isEmpty()) {
                    System.out.println();
                    Util.printMessage("--------------------- Retrying failed snapshots -----------------------------");
                    for (Map.Entry<String, String> entry : failedSnapshotMap.entrySet()) {
                        String snapshotName = entry.getKey();
                        String tableName = entry.getValue();
                        snapshot(zooKeeper, tableName, snapshotName);
                    }
                }

                if (args.has(Args.OPTION_DELETE_SNAPSHOT_FOR_NOT_EXISTING_TABLE)) {
                    deleteSnapshotsForNotExistingTables();
                }
            }
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

    private void deleteSnapshotsForNotExistingTables() throws IOException {
        List<SnapshotDescription> snapshots = admin.listSnapshots();
        for (SnapshotDescription snapshot : snapshots) {
            String tableName = snapshot.getTable();
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

    private void clearWatchLeak(Args args, ZooKeeper zooKeeper)
            throws IOException, InterruptedException, KeeperException {
        String zookeeperNodeArg = args.getZookeeperQuorum().split(",")[0];
        String zookeeperNode = zookeeperNodeArg.split(":")[0];
        String zookeeperPort = zookeeperNodeArg.split(":").length > 1 ? zookeeperNodeArg.split(":")[1] : "2181";
        String command = "echo \"wchc\" | nc " + zookeeperNode + " "
                + zookeeperPort + " | grep online-snapshot | sort | uniq";
        Process exec = Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
        exec.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));

        List<String> watchNameList = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            watchNameList.add(line.trim());
        }
        for (String watchName : watchNameList) {
            System.out.println(watchName);
        }
        if (watchNameList.size() > 0) {
            System.out.print("There are " + watchNameList.size() + " watch leaks. Clear them all. ");
            boolean proceed = Util.askProceed();
            if (proceed) {
                for (String watchName : watchNameList) {
                    if (watchName.startsWith(ABORT_WATCH_PREFIX)) {
                        String snapshotName = watchName.replace(ABORT_WATCH_PREFIX, "");
                        clearAbortWatchLeak(zooKeeper, snapshotName);
                    }
                }
            }
        } else {
            System.out.print("There is no watch leak.");
        }
    }

    private String errorMessage(Throwable e) {
        return "Snapshot Failed - " + e.getMessage();
    }

    String getPrefix(String tableName) {
        return tableName.replace(":", "_") + TIMESTAMP_PREFIX;
    }

    @VisibleForTesting
    void snapshot(ZooKeeper zooKeeper, String tableName, String snapshotName)
            throws IOException, KeeperException, InterruptedException {
        if (args.has(Args.OPTION_TEST) && !tableName.startsWith("UNIT_TEST_")) return;

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
    }

    private void clearAbortWatchLeak(ZooKeeper zooKeeper, String snapshotName)
            throws KeeperException, InterruptedException {
        if (zooKeeper != null && zooKeeper.getState().isConnected()) {
            String path = ABORT_WATCH_PREFIX + snapshotName;
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
        }
    }

    private String timestamp(TimestampFormat format) {
        if (format == TimestampFormat.log) {
            return DATE_FORMAT_LOG.format(System.currentTimeMillis());
        } else {
            return DATE_FORMAT_SNAPSHOT.format(System.currentTimeMillis());
        }
    }

    private void deleteOldSnapshots(HBaseAdmin admin, String tableName) throws IOException {
        if (args.keepCount(tableName) == SnapshotArgs.KEEP_UNLIMITED
                || args.has(Args.OPTION_TEST) && !tableName.startsWith("UNIT_TEST_")) {
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

    private boolean exists(HBaseAdmin admin, String targetSnapshotName) throws IOException {
        List<SnapshotDescription> sd = SnapshotAdapter.getSnapshotDescriptions(admin, targetSnapshotName);
        return sd.size() > 0;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }

    private enum TimestampFormat {log, snapshot}
}
