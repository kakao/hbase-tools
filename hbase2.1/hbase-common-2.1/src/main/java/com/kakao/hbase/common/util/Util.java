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

package com.kakao.hbase.common.util;

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.InvalidTableException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class Util {
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        Util.setLoggingThreshold("ERROR");
    }

    private Util() {
    }

    public static void setLoggingThreshold(String loggingLevel) {
        Properties props = new Properties();
        props.setProperty("log4j.threshold", loggingLevel);
        PropertyConfigurator.configure(props);
    }

    /**
     * @param tableName Should be string, not TableName
     */
    public static void validateTable(Admin admin, String tableName) throws IOException, InterruptedException {
        if (tableName.equals(Args.ALL_TABLES)) return;

        boolean tableExists = false;
        try {
            if (tableName.contains(Constant.TABLE_DELIMITER)) {
                String[] tables = tableName.split(Constant.TABLE_DELIMITER);
                for (String table : tables) {
                    tableExists = admin.tableExists(TableName.valueOf(table));
                }
            } else {
                tableExists = admin.listTables(tableName).length > 0;
            }
        } catch (Exception e) {
            Thread.sleep(1000);
            System.out.println();
            System.out.println(admin.getConfiguration().get("hbase.zookeeper.quorum") + " is invalid zookeeper quorum");
            System.exit(1);
        }
        if (tableExists) {
            try {
                if (!admin.isTableEnabled(TableName.valueOf(tableName))) {
                    throw new InvalidTableException("Table is not enabled.");
                }
            } catch (Exception ignore) {
            }
        } else {
            throw new InvalidTableException("Table does not exist.");
        }
    }

    public static boolean isMoved(Connection connection, TableName tableName, String regionName, String serverNameTarget) throws IOException {
        RegionLocator regionLocator = connection.getRegionLocator(tableName);
        List<HRegionLocation> regionLocations = regionLocator.getAllRegionLocations();
        for (HRegionLocation regionLocation : regionLocations) {
            if (regionLocation.getRegion().getEncodedName().equals(regionName)) {
                return regionLocation.getServerName().getServerName().equals(serverNameTarget);
            }
        }

        if (!existsRegion(regionName, regionLocations))
            return true; // skip moving
        return false;
    }

    private static boolean existsRegion(String regionName, List<HRegionLocation> regionLocations) {
        boolean regionExists = false;
        for (HRegionLocation regionLocation : regionLocations) {
            if (regionLocation.getRegion().getEncodedName().equals(regionName))
                regionExists = true;
        }
        return regionExists;
    }

    public static boolean askProceed() {
        System.out.print("Proceed (Y or N)? ");
        Scanner scanner = new Scanner(System.in);
        String s = scanner.nextLine();
        return s.toUpperCase().equals("Y");
    }

    public static long printVerboseMessage(Args args, String message, long startTimestamp) {
        long currentTimestamp = System.currentTimeMillis();
        if (args != null && args.has(Args.OPTION_VERBOSE)) {
            System.out.println(now() + " - " + message + " - Duration(ms) - " + (currentTimestamp - startTimestamp));
        }
        return currentTimestamp;
    }

    public static void printVerboseMessage(Args args, String message) {
        if (args != null && args.has(Args.OPTION_VERBOSE))
            System.out.println(now() + " - " + message);
    }

    public static void printMessage(String message) {
        System.out.println(now() + " - " + message);
    }

    private static String now() {
        return DATE_FORMAT.format(System.currentTimeMillis());
    }

    public static String getResource(String rsc) throws IOException {
        StringBuilder sb = new StringBuilder();

        ClassLoader cLoader = Util.class.getClassLoader();
        try (InputStream i = cLoader.getResourceAsStream(rsc)) {
            assert i != null;
            BufferedReader r = new BufferedReader(new InputStreamReader(i));

            String l;
            while ((l = r.readLine()) != null) {
                sb.append(l).append("\n");
            }
        }
        return sb.toString();
    }

    private static String readString(Reader reader) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            char[] buffer = new char[1000];
            int read;
            while ((read = bufferedReader.read(buffer)) != -1) {
                sb.append(buffer, 0, read);
            }
        }
        return sb.toString();
    }

    public static String readFromResource(String fileName) throws IOException {
        try (InputStream in = Util.class.getClassLoader().getResourceAsStream(fileName)) {
            assert in != null;
            return readString(new InputStreamReader(in));
        }
    }

    public static String readFromFile(String fileName) throws IOException {
        return readString(new FileReader(fileName));
    }

    public static boolean isFile(String path) {
        File file = new File(path);
        return file.exists() && !file.isDirectory();
    }


    public static Set<TableName> parseTableSet(Admin admin, Args args) throws IOException {
        Set<TableName> tableSet = new HashSet<>();
        String tableArg = (String) args.getOptionSet().nonOptionArguments().get(1);
        if (tableArg.contains(Constant.TABLE_DELIMITER)) {
            String[] tableArgs = tableArg.split(Constant.TABLE_DELIMITER);
            for (String arg : tableArgs) {
                for (HTableDescriptor hTableDescriptor : admin.listTables(arg)) {
                    tableSet.add(hTableDescriptor.getTableName());
                }
            }
        } else {
            for (HTableDescriptor hTableDescriptor : admin.listTables(tableArg)) {
                TableName tableName = hTableDescriptor.getTableName();
                if (args.has(Args.OPTION_TEST) && !tableName.getNameAsString().startsWith("UNIT_TEST_")) {
                    continue;
                }
                tableSet.add(tableName);
            }
        }
        return tableSet;
    }

    public static String getRegionInfoString(RegionInfo regionA) {
        return "{TABLE => " + regionA.getTable().getNameWithNamespaceInclAsString()
                + ", ENCODED => " + regionA.getEncodedName()
                + ", STARTKEY => '" + Bytes.toStringBinary(regionA.getStartKey())
                + "', ENDKEY => '" + Bytes.toStringBinary(regionA.getEndKey()) + "'";
    }

    public static void sendAlertAfterFailed(Args args, Class clazz, String message) {
        if (args != null && args.getAfterFailureScript() != null)
            AlertSender.send(args.getAfterFailureScript(),
                    "FAIL - " + clazz.getSimpleName()
                            + " - " + message
                            + " - " + args.toString());
        sendAlertAfterFinish(args, clazz, message, false);
    }

    public static void sendAlertAfterSuccess(Args args, Class clazz) {
        sendAlertAfterSuccess(args, clazz, null);
    }

    @SuppressWarnings("SameParameterValue")
    private static void sendAlertAfterSuccess(Args args, Class clazz, String message) {
        if (args != null && args.getAfterSuccessScript() != null)
            AlertSender.send(args.getAfterSuccessScript(),
                    "SUCCESS - " + clazz.getSimpleName()
                            + (message == null || message.equals("") ? "" : " - " + message)
                            + " - " + args.toString());
        sendAlertAfterFinish(args, clazz, message, true);
    }

    private static void sendAlertAfterFinish(Args args, Class clazz, String message, boolean success) {
        if (args != null && args.getAfterFinishScript() != null)
            AlertSender.send(args.getAfterFinishScript(),
                    (success ? "SUCCESS - " : "FAIL - ") + clazz.getSimpleName()
                            + (message == null || message.equals("") ? "" : " - " + message)
                            + " - " + args.toString());
    }

    public static String getMethodName() {
        StackTraceElement current = Thread.currentThread().getStackTrace()[2];
        return current.getClassName() + "." + current.getMethodName();
    }

    public static long parseTimestamp(String timestampString) {
        try {
            Date date = Constant.DATE_FORMAT_ARGS.parse(timestampString);
            if (date.compareTo(new Date(System.currentTimeMillis())) > 0)
                throw new IllegalArgumentException(Constant.MESSAGE_INVALID_DATE_FORMAT);
            return date.getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException(Constant.MESSAGE_INVALID_DATE_FORMAT);
        }
    }

    public static NavigableMap<RegionInfo, ServerName> getRegionLocationsMap(Connection connection, TableName tableName) throws IOException {
        final NavigableMap<RegionInfo, ServerName> result = new TreeMap<>();
        try(RegionLocator rl = connection.getRegionLocator(tableName)) {
            for (HRegionLocation e : rl.getAllRegionLocations()) {
                result.put(e.getRegion(), e.getServerName());
            }
        }
        return result;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean askProceedInteractively(Args args, boolean printNewLine) {
        if (args.has(Args.OPTION_INTERACTIVE)) {
            if (args.has(Args.OPTION_FORCE_PROCEED)) {
                if (printNewLine) System.out.println();
            } else {
                System.out.print(" - ");
                return askProceed();
            }
        } else {
            if (printNewLine) System.out.println();
        }
        return true;
    }
}
