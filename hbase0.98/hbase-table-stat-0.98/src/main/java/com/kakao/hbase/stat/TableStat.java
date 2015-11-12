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

import com.google.common.annotations.VisibleForTesting;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.HBaseClient;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.stat.load.*;
import com.kakao.hbase.stat.print.Formatter;
import com.kakao.hbase.stat.webapp.WebApp;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TableStat {
    private final TableInfo tableInfo;
    private final int intervalMS;
    private final Formatter formatter;
    private final Args args;
    private final WebApp webApp;
    private boolean paused = false;

    public TableStat(HBaseAdmin admin, Args args) throws Exception {
        intervalMS = args.getIntervalMS();
        tableInfo = new TableInfo(admin, args.getTableName(), args);
        formatter = new Formatter(args.getTableName(), tableInfo.getLoad());
        this.args = args;

        webApp = WebApp.getInstance(args, this);
        webApp.startHttpServer();
    }

    public static void main(String[] argsParam) throws Exception {
        try {
            Args args = new StatArgs(argsParam);

            try (HBaseAdmin admin = HBaseClient.getAdmin(args)) {
                TableStat tableStat = new TableStat(admin, args);
                tableStat.run();
                tableStat.exit(0, null);
            } catch (Throwable e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        } catch (Throwable e) {
            System.out.println(usage());
        }
    }

    public static String usage() {
        return "Show some important metrics periodically.\n"
            + "usage: hbase-table-stat (<zookeeper quorum>|<args file>) [table] [options]\n"
            + "  options:\n"
            + "    --" + Args.OPTION_INTERVAL + "=<secs> : Iteration interval in seconds. Default 10 secs.\n"
            + "    --" + Args.OPTION_REGION + ": Stats on region level.\n"
            + "    --" + Args.OPTION_REGION_SERVER + "=<rs name regex> : "
            + "Show stats of specific region server at region level.\n"
            + "    --" + Args.OPTION_OUTPUT + "=<file name> : Save stats into a file with CSV format.\n"
            + "    --" + Args.OPTION_HTTP_PORT + "=<http port> : Http server port. Default 0.\n"
            + dynamicOptions()
            + Args.commonUsage();
    }

    public static String dynamicOptions() {
        return "  dynamic options:\n"
            + "    h - show this help message\n"
            + "    q - quit this app\n"
            + "    p - pause iteration. toggle\n"
            + "    d - show differences from the start. toggle\n"
            + "    R - reset diff start point to now\n"
            + "    c - show changed records only. toggle\n"
            + "    r - show change rate instead of diff. toggle\n"
            + "    [shift]0-9 - sort by selected column value or diff (with shift). in ascending order\n"
            + "    S - save current load data to a csv file\n"
            + "    L - load a saved csv file and set it as diff start point\n"
            + "    C - show connection information\n";
    }

    public void run() throws Exception {
        try {
            if (intervalMS < 0) {
                throw new IllegalArgumentException("intervalMS is invalid - " + intervalMS);
            } else if (intervalMS == 0) {
                runInternal();
            } else {
                runKeyInputListener();

                //noinspection InfiniteLoopStatement
                while (true) {
                    long timestamp = System.currentTimeMillis();
                    if (!paused) runInternal();
                    long duration = System.currentTimeMillis() - timestamp;
                    long sleepMillis = intervalMS - duration;
                    sleepMillis = sleepMillis < 0 ? 0 : sleepMillis;
                    Util.printVerboseMessage(args, "Sleep " + sleepMillis + " milliseconds");
                    Thread.sleep(sleepMillis);
                }
            }
            Util.sendAlertAfterFinished(args, this.getClass());
        } catch (Throwable e) {
            Util.sendAlertAfterFailed(args, this.getClass(), e.getMessage());
            throw e;
        }
    }

    private void runKeyInputListener() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        KeyInputListener keyInputListener = new KeyInputListener(this);
        executorService.execute(keyInputListener);
    }

    private void runInternal() throws Exception {
        try {
            synchronized (this) {
                tableInfo.refresh();
            }

            printStat();
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            exit(1, e);
        }
    }

    void exit(int exitCode, Throwable e) {
        if (exitCode == 0) {
            Util.sendAlertAfterFinished(args, this.getClass());
        } else {
            Util.sendAlertAfterFailed(args, this.getClass(), e.getMessage());
        }
        System.exit(exitCode);
    }

    void printStat() {
        System.out.println(formatter.toString());
    }

    public Formatter getFormatter() {
        return formatter;
    }

    @VisibleForTesting
    int getIntervalMS() {
        return intervalMS;
    }

    public String toggleDiffFromStart() {
        synchronized (this) {
            boolean diffFromStart = formatter.toggleDiffFromStart();
            return "Toggle DiffFromStart to " + diffFromStart + "\n";
        }
    }

    public String toggleShowChangedOnly() {
        synchronized (this) {
            boolean showChangedOnly = formatter.toggleShowChangedOnly();
            return "Toggle ShowChangedOnly to " + showChangedOnly + "\n";
        }
    }

    public void pause() {
        synchronized (this) {
            paused = true;
        }
    }

    public void resume() {
        synchronized (this) {
            paused = false;
        }
    }

    public String togglePause() {
        synchronized (this) {
            paused = !paused;

            if (paused)
                return "Paused\n";
            else
                return "Resumed\n";
        }
    }

    public String resetDiffStartPoint() {
        synchronized (this) {
            getLoad().resetDiffStartPoint();
            return "Reset DiffStartPoint\n";
        }
    }

    public String toggleShowRate() {
        synchronized (this) {
            boolean showRate = getLoad().toggleShowRate();
            return "Toggle ShowRate to " + showRate + "\n";
        }
    }

    public String showConnectionInfo() {
        return "Connected to " + args.getZookeeperQuorum() + "\n" + webApp.printInetAddresses();
    }

    public String setSort(String sortKeyString) {
        synchronized (this) {
            try {
                tableInfo.getLoad().setSortKey(new SortKey(sortKeyString));
                return "Sort by " + getLoad().getSortKeyInfo() + "\n";
            } catch (Exception e) {
                return sortKeyString + " is an invalid sort key.\n";
            }
        }
    }

    public String save() {
        synchronized (this) {
            return getLoad().save(args);
        }
    }

    public String showFiles() {
        synchronized (this) {
            return getLoad().showFiles(args);
        }
    }

    public void load(String input) {
        synchronized (this) {
            getLoad().load(args, input);
        }
    }

    @VisibleForTesting
    TableInfo getTableInfo() {
        return tableInfo;
    }

    @VisibleForTesting
    Load getLoad() {
        return tableInfo.getLoad();
    }

    @VisibleForTesting
    LoadRecord getLoad(Level level) {
        return tableInfo.getLoad().getLoadMap().get(level);
    }
}
