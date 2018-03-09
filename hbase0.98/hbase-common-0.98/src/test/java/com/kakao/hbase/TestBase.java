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
import com.kakao.hbase.specific.HBaseAdminWrapper;
import joptsimple.OptionParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
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
    protected static final int MAX_WAIT_ITERATION = 200;
    protected static final long WAIT_INTERVAL = 100;
    private static final List<String> additionalTables = new ArrayList<>();
    private static final Log LOG = LogFactory.getLog(TestBase.class);
    protected static int RS_COUNT = 2;
    protected static HBaseAdmin admin = null;
    protected static Configuration conf = null;
    protected static String tableName;
    protected static HConnection hConnection;
    protected static boolean miniCluster = false;
    protected static boolean securedCluster = false;
    protected static User USER_RW = null;
    protected static HBaseTestingUtility hbase = null;
    private static Map<String, Map<String, Long>> tableServerWriteRequestMap = new HashMap<>();
    private static boolean previousBalancerRunning = true;
    private static ArrayList<ServerName> serverNameList = null;
    private static boolean testNamespaceCreated = false;
    public final String tablePrefix;
    @Rule
    public final TestName testName = new TestName();

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
                admin = new HBaseAdminWrapper(conf);
            }
        } else {
            if (admin == null) {
                final String argsFileName = securedCluster ? "../../testClusterRealSecured.args" : "../../testClusterRealNonSecured.args";
                if (!Util.isFile(argsFileName)) {
                    throw new IllegalStateException("You have to define args file " + argsFileName + " for tests.");
                }

                String[] testArgs = {argsFileName};
                Args args = new TestArgs(testArgs);
                admin = HBaseClient.getAdmin(args);
                conf = admin.getConfiguration();
                RS_COUNT = getServerNameList().size();
            }
        }
        previousBalancerRunning = admin.setBalancerRunning(false, true);
        hConnection = HConnectionManager.createConnection(conf);

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
            dropNamespace(admin, TEST_NAMESPACE);
            if (previousBalancerRunning) {
                if (admin != null)
                    admin.setBalancerRunning(true, true);
            }
        }
    }

    protected static void createNamespace(HBaseAdmin admin, String namespaceName) throws IOException {
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespaceName).build();
        try {
            admin.createNamespace(nd);
            testNamespaceCreated = true;
        } catch (NamespaceExistException ignore) {
        }
    }

    protected static void dropNamespace(HBaseAdmin admin, String namespaceName) throws IOException {
        if (!testNamespaceCreated) return;

        try {
            admin.deleteNamespace(namespaceName);
            testNamespaceCreated = false;
        } catch (NamespaceNotFoundException ignore) {
        }
    }

    protected static void validateTable(HBaseAdmin admin, String tableName) throws IOException, InterruptedException {
        if (tableName.equals(Args.ALL_TABLES)) return;

        boolean tableExists = admin.tableExists(tableName);
        if (tableExists) {
            if (!admin.isTableEnabled(tableName)) {
                throw new InvalidTableException("Table is not enabled.");
            }
        } else {
            throw new InvalidTableException("Table does not exist.");
        }
    }

    protected static List<HRegionInfo> getRegionInfoList(ServerName serverName, String tableName) throws IOException {
        List<HRegionInfo> onlineRegions = new ArrayList<>();
        for (HRegionInfo onlineRegion : admin.getOnlineRegions(serverName)) {
            if (onlineRegion.getTable().getNameAsString().equals(tableName)) {
                onlineRegions.add(onlineRegion);
            }
        }
        return onlineRegions;
    }

    protected static ArrayList<ServerName> getServerNameList() throws IOException {
        if (TestBase.serverNameList == null) {
            Set<ServerName> serverNameSet = new TreeSet<>(admin.getClusterStatus().getServers());
            ArrayList<ServerName> serverNameList = new ArrayList<>();
            for (ServerName serverName : serverNameSet) {
                serverNameList.add(serverName);
            }
            TestBase.serverNameList = serverNameList;
        }
        return TestBase.serverNameList;
    }

    protected void move(HRegionInfo regionInfo, ServerName serverName) throws Exception {
        admin.move(regionInfo.getEncodedName().getBytes(), serverName.getServerName().getBytes());
        waitForMoving(regionInfo, serverName);
    }

    @Before
    public void setUp() throws Exception {
        additionalTables.clear();
        deleteSnapshots(tableName);
        tableName = tablePrefix + "_" + testName.getMethodName();
        recreateTable(tableName);
    }

    @After
    public void tearDown() throws Exception {
        dropTable(tableName);
        deleteSnapshots(tableName);
        for (String additionalTable : additionalTables) {
            dropTable(additionalTable);
            deleteSnapshots(additionalTable);
        }
    }

    protected void deleteSnapshots(String tableName) throws Exception {
        for (HBaseProtos.SnapshotDescription snapshotDescription : admin.listSnapshots(".*" + tableName + ".*")) {
            if (snapshotDescription.getTable().equals(tableName) || snapshotDescription.getTable().equals(TEST_NAMESPACE + ":" + tableName)) {
                admin.deleteSnapshots(snapshotDescription.getName());
            }
        }
    }

    protected void dropTable(String tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException ignored) {
            }
            admin.deleteTable(tableName);
        }
    }

    protected void recreateTable(String tableName) throws Exception {
        dropTable(tableName);
        createTable(tableName);
    }

    protected String createAdditionalTable(String tableName) throws Exception {
        recreateTable(tableName);
        additionalTables.add(tableName);
        return tableName;
    }

    protected void createTable(String tableName) throws Exception {
        HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tableName.getBytes()));
        HColumnDescriptor cd = new HColumnDescriptor(TEST_TABLE_CF.getBytes());
        td.addFamily(cd);
        admin.createTable(td);
        LOG.info(tableName + " table is successfully created.");
    }

    protected void splitTable(byte[] splitPoint) throws Exception {
        splitTable(tableName, splitPoint);
    }

    protected void splitTable(String tableName, byte[] splitPoint) throws Exception {
        int regionCount = getRegionCount(tableName);
        admin.split(tableName.getBytes(), splitPoint);
        waitForSplitting(tableName, regionCount + 1);
    }

    protected int getRegionCount(String tableName) throws IOException {
        return admin.getTableRegions(tableName.getBytes()).size();
    }

    protected ArrayList<HRegionInfo> getRegionInfoList(String tableName) throws IOException {
        Set<HRegionInfo> hRegionInfoSet = new TreeSet<>();
        try (HTable table = (HTable) hConnection.getTable(tableName)) {
            hRegionInfoSet.addAll(table.getRegionLocations().keySet());
        }
        return new ArrayList<>(hRegionInfoSet);
    }

    protected void waitForMoving(HRegionInfo hRegionInfo, ServerName serverName) throws Exception {
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

    protected void waitForSplitting(int regionCount) throws IOException, InterruptedException {
        waitForSplitting(tableName, regionCount);
    }

    protected void waitForSplitting(String tableName, int regionCount) throws IOException, InterruptedException {
        int regionCountActual = 0;
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            try (HTable table = (HTable) hConnection.getTable(tableName)) {
                regionCountActual = 0;
                NavigableMap<HRegionInfo, ServerName> regionLocations = table.getRegionLocations();
                for (Map.Entry<HRegionInfo, ServerName> entry : regionLocations.entrySet()) {
                    ServerLoad serverLoad = admin.getClusterStatus().getLoad(entry.getValue());
                    for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                        if (Arrays.equals(entry.getKey().getRegionName(), regionLoad.getName()))
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

    protected void updateWritingRequestMetric(String tableName, ServerName serverName) throws IOException {
        Map<String, Long> serverMap = tableServerWriteRequestMap.get(tableName);
        if (serverMap == null) {
            serverMap = new HashMap<>();
        }

        long writeRequestCountActual = getWriteRequestCountActual(tableName, serverName);
        serverMap.put(serverName.getServerName(), writeRequestCountActual);
        tableServerWriteRequestMap.put(tableName, serverMap);
    }

    private long getWriteRequestCountActual(String tableName, ServerName serverName) throws IOException {
        long writeRequestCountActual;
        try (HTable table = (HTable) hConnection.getTable(tableName)) {
            writeRequestCountActual = 0;
            NavigableMap<HRegionInfo, ServerName> regionLocations = table.getRegionLocations();
            for (Map.Entry<HRegionInfo, ServerName> entry : regionLocations.entrySet()) {
                if (serverName.equals(entry.getValue())) {
                    ServerLoad serverLoad = admin.getClusterStatus().getLoad(entry.getValue());
                    for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                        if (Arrays.equals(entry.getKey().getRegionName(), regionLoad.getName()))
                            writeRequestCountActual += regionLoad.getWriteRequestsCount();
                    }
                }
            }
        }

        Long aLong = getWriteRequestMetric(tableName, serverName);
        return writeRequestCountActual - aLong;
    }

    private Long getWriteRequestMetric(String tableName, ServerName serverName) {
        Map<String, Long> serverMap = tableServerWriteRequestMap.get(tableName);
        if (serverMap == null) {
            serverMap = new HashMap<>();
            tableServerWriteRequestMap.put(tableName, serverMap);
        }

        Long writeRequest = serverMap.get(serverName.getServerName());
        if (writeRequest == null) {
            writeRequest = 0L;
            serverMap.put(serverName.getServerName(), writeRequest);
        }
        return writeRequest;
    }

    protected void waitForWriting(String tableName, long writeRequestCount) throws Exception {
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

    private long getWriteRequestCountActual(String tableName) throws IOException {
        long writeRequestCountActual;
        try (HTable table = (HTable) hConnection.getTable(tableName)) {
            writeRequestCountActual = 0;
            NavigableMap<HRegionInfo, ServerName> regionLocations = table.getRegionLocations();
            for (Map.Entry<HRegionInfo, ServerName> entry : regionLocations.entrySet()) {
                ServerLoad serverLoad = admin.getClusterStatus().getLoad(entry.getValue());
                for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                    if (Arrays.equals(entry.getKey().getRegionName(), regionLoad.getName()))
                        writeRequestCountActual += regionLoad.getWriteRequestsCount();
                }
            }
        }
        return writeRequestCountActual;
    }

    protected void waitForWriting(String tableName, ServerName serverName, long writeRequestCount) throws Exception {
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

    protected void waitForDisabled(String tableName) throws Exception {
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            if (admin.isTableDisabled(tableName)) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.fail(Util.getMethodName() + " failed");
    }

    protected void waitForEnabled(String tableName) throws Exception {
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            if (admin.isTableEnabled(tableName)) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.fail(Util.getMethodName() + " failed");
    }

    protected void waitForDelete(String tableName) throws Exception {
        for (int i = 0; i < MAX_WAIT_ITERATION; i++) {
            if (!admin.tableExists(tableName)) {
                return;
            }
            Thread.sleep(WAIT_INTERVAL);
        }
        Assert.fail(Util.getMethodName() + " failed");
    }

    protected List<HBaseProtos.SnapshotDescription> listSnapshots(String tableName) throws IOException {
        return admin.listSnapshots(tableName);
    }

    protected void mergeRegion(HRegionInfo regionA, HRegionInfo regionB) throws IOException, InterruptedException {
        mergeRegion(tableName, regionA, regionB);
    }

    protected void mergeRegion(String tableName, HRegionInfo regionA, HRegionInfo regionB) throws IOException, InterruptedException {
        int size = getRegionInfoList(tableName).size();
        admin.mergeRegions(regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
        waitForSplitting(tableName, size - 1);
    }

    protected RegionLoad getRegionLoad(HRegionInfo regionInfo, ServerName serverName) throws IOException {
        ServerLoad serverLoad = admin.getClusterStatus().getLoad(serverName);
        Map<byte[], RegionLoad> regionsLoad = serverLoad.getRegionsLoad();
        for (Map.Entry<byte[], RegionLoad> entry : regionsLoad.entrySet()) {
            if (Arrays.equals(entry.getKey(), regionInfo.getRegionName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    protected HTable getTable(String tableName) throws IOException {
        return (HTable) hConnection.getTable(tableName);
    }

    public static class TestArgs extends Args {
        public TestArgs(String[] args) throws IOException {
            super(args);
        }

        @Override
        protected OptionParser createOptionParser() {
            return createCommonOptionParser();
        }
    }
}
