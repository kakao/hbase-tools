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

package com.kakao.hbase;

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.HBaseClient;
import com.kakao.hbase.common.InvalidTableException;
import com.kakao.hbase.common.util.Util;
import joptsimple.OptionParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;

public class TestBase extends SecureTestUtil {
    protected static final String TEST_TABLE_CF = "d";
    protected static final String TEST_TABLE_CF2 = "e";
    protected static final String TEST_NAMESPACE = "unit_test";
    private static final int MAX_WAIT_ITERATION = 200;
    private static final long WAIT_INTERVAL = 100;
    private static final List<TableName> additionalTables = new ArrayList<>();
    private static final Log LOG = LogFactory.getLog(TestBase.class);
    protected static int RS_COUNT = 2;
    protected static Configuration conf = null;
    protected static TableName tableName;
    protected static Connection connection;
    protected static Admin admin;
    protected static boolean miniCluster = false;
    protected static boolean securedCluster = false;
    protected static User USER_RW = null;
    protected static HBaseTestingUtility hbase = null;
    private static Map<TableName, Map<String, Long>> tableServerWriteRequestMap = new HashMap<>();
    private static boolean previousBalancerRunning = true;
    private static ArrayList<ServerName> serverNameList = null;
    private static boolean testNamespaceCreated = false;
    @Rule
    public final TestName testName = new TestName();
    private final String tablePrefix;

    public TestBase(Class c) {
        tablePrefix = Constant.UNIT_TEST_TABLE_PREFIX + c.getSimpleName();
    }

    @BeforeClass
    public static void setUpOnce() throws Exception {
        miniCluster = System.getProperty("cluster.type").equals("mini");
        securedCluster = System.getProperty("cluster.secured").equals("true");
        System.out.println("realCluster - " + !miniCluster);
        System.out.println("securedCluster - " + securedCluster);

        Util.setLoggingThreshold("ERROR");

        if (miniCluster) {
            if (hbase == null) {
                conf = HBaseConfiguration.create(new Configuration(true));
                conf.setInt("hbase.master.info.port", -1);
                conf.set("zookeeper.session.timeout", "3600000");
                conf.set("dfs.client.socket-timeout", "3600000");
                conf.set("hbase.zookeeper.property.maxSessionTimeout", "3600000");
                hbase = new HBaseTestingUtility(conf);

                if (securedCluster) {
                    enableSecurity(conf);
                    verifyConfiguration(conf);
                    hbase.startMiniCluster(RS_COUNT);
                    hbase.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME.getName(), 10000);
                } else {
                    hbase.startMiniCluster(RS_COUNT);
                }
                updateTestZkQuorum();
            }
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } else {
            if (connection == null) {
                final String argsFileName = securedCluster ? "../../testClusterRealSecured.args" : "../../testClusterRealNonSecured.args";
                if (!Util.isFile(argsFileName)) {
                    throw new IllegalStateException("You have to define args file " + argsFileName + " for tests.");
                }

                String[] testArgs = {argsFileName};
                Args args = new TestArgs(testArgs);
                connection = HBaseClient.getConnection(args);
                admin = connection.getAdmin();
                conf = connection.getConfiguration();
                RS_COUNT = getServerNameList().size();
            }
        }
        previousBalancerRunning = admin.balancerSwitch(false, true);
        createNamespace(admin, TEST_NAMESPACE);

        USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    }

    private static void updateTestZkQuorum() {
        String newZkQuorum = hbase.getConfiguration().get("hbase.zookeeper.quorum") + ":" + hbase.getZkCluster().getClientPort();
        conf.set("hbase.zookeeper.quorum", newZkQuorum);
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
        if (!miniCluster) {
            dropNamespace(TEST_NAMESPACE);
            if (previousBalancerRunning) {
                try(Admin admin = connection.getAdmin()) {
                    admin.balancerSwitch(true, true);
                }
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void createNamespace(Admin admin, String namespaceName) throws IOException {
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespaceName).build();
        try {
            admin.createNamespace(nd);
            testNamespaceCreated = true;
        } catch (NamespaceExistException ignore) {
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void dropNamespace(String namespaceName) throws IOException {
        if (!testNamespaceCreated) return;

        try {
            admin.deleteNamespace(namespaceName);
            testNamespaceCreated = false;
        } catch (NamespaceNotFoundException ignore) {
        }
    }

    protected static void validateTable(TableName tableName) throws IOException {
        if (tableName.getNameAsString().equals(Args.ALL_TABLES)) return;

        boolean tableExists = admin.tableExists(tableName);
        if (tableExists) {
            if (!admin.isTableEnabled(tableName)) {
                throw new InvalidTableException("Table is not enabled.");
            }
        } else {
            throw new InvalidTableException("Table does not exist.");
        }
    }

    protected static List<RegionInfo> getRegionInfoList(ServerName serverName, TableName tableName) throws IOException {
        List<RegionInfo> onlineRegions = new ArrayList<>();
        for (RegionInfo onlineRegion : admin.getRegions(serverName)) {
            if (onlineRegion.getTable().equals(tableName)) {
                onlineRegions.add(onlineRegion);
            }
        }
        return onlineRegions;
    }

    @SuppressWarnings("deprecation")
    protected static ArrayList<ServerName> getServerNameList() throws IOException {
        if (TestBase.serverNameList == null) {
            Set<ServerName> serverNameSet = new TreeSet<>(admin.getClusterStatus().getServers());
            TestBase.serverNameList = new ArrayList<>(serverNameSet);
        }
        return TestBase.serverNameList;
}

    protected void move(RegionInfo regionInfo, ServerName serverName) throws Exception {
        admin.move(regionInfo.getEncodedName().getBytes(), serverName.getServerName().getBytes());
        waitForMoving(regionInfo, serverName);
    }

    @Before
    public void setUp() throws Exception {
        additionalTables.clear();
        deleteSnapshots(tableName);
        tableName = TableName.valueOf(tablePrefix + "_" + testName.getMethodName());
        recreateTable(tableName);
    }

    @After
    public void tearDown() throws Exception {
        dropTable(tableName);
        deleteSnapshots(tableName);
        for (TableName additionalTable : additionalTables) {
            dropTable(additionalTable);
            deleteSnapshots(additionalTable);
        }
    }

    private void deleteSnapshots(TableName tableName) throws Exception {
        for (SnapshotDescription snapshotDescription : admin.listSnapshots(".*" + tableName + ".*")) {
            if (snapshotDescription.getTableName().equals(tableName) || snapshotDescription.getTableName().getNameAsString().equals(TEST_NAMESPACE + ":" + tableName)) {
                admin.deleteSnapshots(snapshotDescription.getName());
            }
        }
    }

    protected void dropTable(TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException ignored) {
            }
            admin.deleteTable(tableName);
        }
    }

    private void recreateTable(TableName tableName) throws Exception {
        dropTable(tableName);
        createTable(tableName);
    }

    protected TableName createAdditionalTable(TableName tableName) throws Exception {
        recreateTable(tableName);
        additionalTables.add(tableName);
        return tableName;
    }

    protected TableName createAdditionalTable(String tableNameStr) throws Exception {
        TableName tableName = TableName.valueOf(tableNameStr);
        recreateTable(tableName);
        additionalTables.add(tableName);
        return tableName;
    }

    @SuppressWarnings("deprecation")
    protected void createTable(TableName tableName) throws Exception {
        HTableDescriptor td = new HTableDescriptor(tableName);
        HColumnDescriptor cd = new HColumnDescriptor(TEST_TABLE_CF.getBytes());
        td.addFamily(cd);
        admin.createTable(td);
        LOG.info(tableName + " table is successfully created.");
    }

    protected void splitTable(byte[] splitPoint) throws Exception {
        splitTable(tableName, splitPoint);
    }

    protected void splitTable(TableName tableName, byte[] splitPoint) throws Exception {
        int regionCount = getRegionCount(tableName);
        admin.split(tableName, splitPoint);
        waitForSplitting(tableName, regionCount + 1);
    }

    private int getRegionCount(TableName tableName) throws IOException {
        return admin.getRegions(tableName).size();
    }

    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    protected ArrayList<RegionInfo> getRegionInfoList(TableName tableName) throws IOException {
        Set<RegionInfo> hRegionInfoSet = new TreeSet<>();
        try (RegionLocator rl = connection.getRegionLocator(tableName)) {
            for (HRegionLocation allRegionLocation : rl.getAllRegionLocations()) {
                hRegionInfoSet.add(allRegionLocation.getRegion());
            }
        }
        return new ArrayList<>(hRegionInfoSet);
    }

    @SuppressWarnings("deprecation")
    private void waitForMoving(RegionInfo hRegionInfo, ServerName serverName) throws Exception {
        Map<byte[], RegionLoad> regionsLoad = null;
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            ServerLoad load = admin.getClusterStatus().getLoad(serverName);
            regionsLoad = load.getRegionsLoad();
            for (byte[] regionName : regionsLoad.keySet()) {
                if (Arrays.equals(regionName, hRegionInfo.getRegionName())) return;
            }
            admin.move(hRegionInfo.getEncodedNameAsBytes(), serverName.getServerName().getBytes());
            Thread.sleep(WAIT_INTERVAL);
        }

        System.out.println("hRegionInfo = " + Bytes.toString(hRegionInfo.getRegionName()));
        for (Map.Entry<byte[], RegionLoad> entry : regionsLoad.entrySet()) {
            System.out.println("regionsLoad = " + Bytes.toString(entry.getKey()) + " - " + entry.getValue());
        }

        Assert.fail(Util.getMethodName() + " failed");
    }

    protected void waitForSplitting(int regionCount) throws InterruptedException, IOException {
        waitForSplitting(tableName, regionCount);
    }

    @SuppressWarnings("deprecation")
    protected void waitForSplitting(TableName tableName, int regionCount) throws InterruptedException, IOException {
        int regionCountActual = 0;
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            try (RegionLocator rl = connection.getRegionLocator(tableName)) {
                regionCountActual = 0;
                List<HRegionLocation> allRegionLocations = rl.getAllRegionLocations();
                for (HRegionLocation entry : allRegionLocations) {
                    ServerLoad serverLoad = admin.getClusterStatus().getLoad(entry.getServerName());
                    for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                        if (Arrays.equals(entry.getRegion().getRegionName(), regionLoad.getName()))
                            regionCountActual++;
                    }
                }
                if (regionCountActual == regionCount) {
                    return;
                }
            } catch (Throwable ignore) {
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.assertEquals("TestBase.waitForSplitting - failed - ", regionCount, regionCountActual);
    }

    protected void updateWritingRequestMetric(TableName tableName, ServerName serverName) throws IOException {
        Map<String, Long> serverMap = tableServerWriteRequestMap.get(tableName);
        if (serverMap == null) {
            serverMap = new HashMap<>();
        }

        long writeRequestCountActual = getWriteRequestCountActual(tableName, serverName);
        serverMap.put(serverName.getServerName(), writeRequestCountActual);
        tableServerWriteRequestMap.put(tableName, serverMap);
    }

    @SuppressWarnings("deprecation")
    private long getWriteRequestCountActual(TableName tableName, ServerName serverName) throws IOException {
        long writeRequestCountActual;
        try (RegionLocator rl = connection.getRegionLocator(tableName)) {
            writeRequestCountActual = 0;
            List<HRegionLocation> allRegionLocations = rl.getAllRegionLocations();
            for (HRegionLocation entry : allRegionLocations) {
                if (serverName.equals(entry.getServerName())) {
                    ServerLoad serverLoad = admin.getClusterStatus().getLoad(entry.getServerName());
                    for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                        if (Arrays.equals(entry.getRegion().getRegionName(), regionLoad.getName()))
                            writeRequestCountActual += regionLoad.getWriteRequestsCount();
                    }
                }
            }
        }

        Long aLong = getWriteRequestMetric(tableName, serverName);
        return writeRequestCountActual - aLong;
    }

    private Long getWriteRequestMetric(TableName tableName, ServerName serverName) {
        Map<String, Long> serverMap = tableServerWriteRequestMap.computeIfAbsent(tableName, k -> new HashMap<>());
        return serverMap.computeIfAbsent(serverName.getServerName(), k -> 0L);
    }

    protected void waitForWriting(TableName tableName, long writeRequestCount) throws Exception {
        long writeRequestCountActual = 0;
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            writeRequestCountActual = getWriteRequestCountActual(tableName);
            if (writeRequestCountActual == writeRequestCount) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.assertEquals(Util.getMethodName() + " failed - ", writeRequestCount, writeRequestCountActual);
    }

    @SuppressWarnings("deprecation")
    private long getWriteRequestCountActual(TableName tableName) throws IOException {
        long writeRequestCountActual;
        try (RegionLocator rl = connection.getRegionLocator(tableName)) {
            writeRequestCountActual = 0;
            List<HRegionLocation> allRegionLocations = rl.getAllRegionLocations();
            for (HRegionLocation entry : allRegionLocations) {
                ServerLoad serverLoad = admin.getClusterStatus().getLoad(entry.getServerName());
                for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                    if (Arrays.equals(entry.getRegion().getRegionName(), regionLoad.getName()))
                        writeRequestCountActual += regionLoad.getWriteRequestsCount();
                }
            }
        }
        return writeRequestCountActual;
    }

    protected void waitForWriting(TableName tableName, ServerName serverName, long writeRequestCount) throws Exception {
        long writeRequestCountActual = 0;
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            writeRequestCountActual = getWriteRequestCountActual(tableName, serverName);
            if (writeRequestCountActual == writeRequestCount) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.assertEquals(Util.getMethodName() + " failed - ", writeRequestCount, writeRequestCountActual);
    }

    protected void waitForDisabled(TableName tableName) throws Exception {
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            if (admin.isTableDisabled(tableName)) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.fail(Util.getMethodName() + " failed");
    }

    protected void waitForEnabled(TableName tableName) throws Exception {
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            if (admin.isTableEnabled(tableName)) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.fail(Util.getMethodName() + " failed");
    }

    protected void waitForDelete(TableName tableName) throws Exception {
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            if (!admin.tableExists(tableName)) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.fail(Util.getMethodName() + " failed");
    }

    protected List<SnapshotDescription> listSnapshots(String tableName) throws IOException {
        return admin.listSnapshots(tableName);
    }

    protected void mergeRegion(RegionInfo regionA, RegionInfo regionB) throws IOException, InterruptedException {
        mergeRegion(tableName, regionA, regionB);
    }

    protected void mergeRegion(TableName tableName, RegionInfo regionA, RegionInfo regionB) throws IOException, InterruptedException {
        int size = getRegionInfoList(tableName).size();
        admin.mergeRegionsAsync(regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
        waitForSplitting(tableName, size - 1);
    }

    @SuppressWarnings("deprecation")
    protected RegionLoad getRegionLoad(RegionInfo regionInfo, ServerName serverName) throws IOException {
        ServerLoad serverLoad = admin.getClusterStatus().getLoad(serverName);
        Map<byte[], RegionLoad> regionsLoad = serverLoad.getRegionsLoad();
        for (Map.Entry<byte[], RegionLoad> entry : regionsLoad.entrySet()) {
            if (Arrays.equals(entry.getKey(), regionInfo.getRegionName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    protected Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    public static class TestArgs extends Args {
        TestArgs(String[] args) throws IOException {
            super(args);
        }

        @Override
        protected OptionParser createOptionParser() {
            return createCommonOptionParser();
        }
    }
}
