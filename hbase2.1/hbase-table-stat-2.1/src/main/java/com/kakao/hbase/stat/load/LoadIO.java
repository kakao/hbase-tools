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

import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.specific.RegionLoadAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LoadIO {
    public static final String NO_SAVED_FILE = "There is no saved file.";
    static final String DIRECTORY_NAME = "stat_saved";
    static final SimpleDateFormat DATE_FORMAT_SAVE = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private static final String HEADER_TIMESTAMP = "Timestamp";
    private static final char SEPARATOR = ',';
    private static final SimpleDateFormat DATE_FORMAT_PRINT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
    private final Load load;
    private List<String> savedFileNameList = new ArrayList<>();
    private boolean headerWritten = false;

    public LoadIO(Load load) {
        this.load = load;
    }

    public String save(Args args) {
        //noinspection ResultOfMethodCallIgnored
        new File(DIRECTORY_NAME).mkdir();

        String fileName = filename(args);

        saveInternal(fileName, false);

        return fileName + " is saved.\n";
    }

    void saveOutput(Args args) {
        final String outputFileName;
        if (args.has(Args.OPTION_OUTPUT)) {
            outputFileName = (String) args.valueOf(Args.OPTION_OUTPUT);
        } else {
            return;
        }

        if (outputFileName != null)
            saveInternal(outputFileName, true);
    }

    private void saveInternal(String outputFileName, boolean append) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputFileName, append), SEPARATOR)) {
            if (append) {
                if (!headerWritten) {
                    write(writer, Entry.header, null, null);
                    headerWritten = true;
                }
            } else {
                write(writer, Entry.header, null, null);
            }

            Map<Level, LoadRecord> loadMap = load.getLoadMap();
            for (Map.Entry<Level, LoadRecord> entry : loadMap.entrySet()) {
                Level level = entry.getKey();
                LoadRecord loadRecord = entry.getValue();

                write(writer, Entry.body, level, loadRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void write(CSVWriter writer, Entry entry, Level level, LoadRecord loadRecord) {
        String[] record = new String[RegionLoadAdapter.loadEntries.length + 2];
        if (entry == Entry.header) {
            record[0] = load.getLevelClass().getLevelTypeString();
            record[1] = HEADER_TIMESTAMP;
        } else {
            record[0] = level.toString();
            record[1] = String.valueOf(load.getTimestampIteration());
        }
        for (LoadEntry loadEntry : RegionLoadAdapter.loadEntries) {
            if (entry == Entry.header) {
                record[RegionLoadAdapter.loadEntryOrdinal(loadEntry) + 2] = loadEntry.name();
            } else {
                Number number = loadRecord.get(loadEntry);
                record[RegionLoadAdapter.loadEntryOrdinal(loadEntry) + 2] = number == null ? "" : number.toString();
            }
        }
        writer.writeNext(record);
    }

    private String safeString(String string) {
        return string.replaceAll("[:,]", "_");
    }

    @VisibleForTesting
    String filename(Args args) {
        StringBuilder sb = prefix(args);
        sb.append(DATE_FORMAT_SAVE.format(load.getTimestampIteration())).append(".csv");
        return DIRECTORY_NAME + "/" + sb.toString();
    }

    private StringBuilder prefix(Args args) {
        String zookeeperQuorum = safeString(args.getZookeeperQuorum());
        String tableName = safeString(args.getTableNamePattern());

        StringBuilder sb = new StringBuilder();
        sb.append(zookeeperQuorum).append("_");
        if (!tableName.equals(Args.ALL_TABLES)) sb.append(tableName);
        sb.append("_");
        return sb;
    }

    public void load(Args args, String input) {
        String fileName = getSavedFileName(input);
        if (fileName == null) return;

        try (CSVReader reader = new CSVReader(new FileReader(fileName), SEPARATOR)) {
            List<LoadEntry> savedLoadEntryList = new ArrayList<>();
            readHeader(reader, savedLoadEntryList);

            Map<Level, LoadRecord> loadedLoadMap = new TreeMap<>();
            readBody(reader, loadedLoadMap, savedLoadEntryList);

            load.setLoadMapStart(loadedLoadMap, getTimestamp(args, fileName));
            System.out.println(fileName + " is loaded.");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @VisibleForTesting
    long getTimestamp(Args args, String fileName) throws ParseException {
        String timestamp = parseTimestamp(args, fileName.replaceAll(DIRECTORY_NAME + "/", ""));
        return DATE_FORMAT_SAVE.parse(timestamp).getTime();
    }

    private void readBody(CSVReader reader, Map<Level, LoadRecord> loadedLoadMap, List<LoadEntry> savedLoadEntryList) throws IOException {
        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            Level level = null;
            for (Level levelEntry : load.getLoadMap().keySet()) {
                if (levelEntry.equalsName(nextLine[0])) {
                    level = levelEntry;
                }
            }
            if (level != null) {
                LoadRecord loadRecord = loadedLoadMap.get(level);
                if (loadRecord == null) {
                    loadRecord = new LoadRecord();
                    loadedLoadMap.put(level, loadRecord);
                }

                int i = 2;
                for (LoadEntry loadEntry : savedLoadEntryList) {
                    String string = nextLine[i++];
                    if (string.length() > 0)
                        loadRecord.put(loadEntry, loadEntry.toNumber(string));
                }
            }
        }
    }

    private void readHeader(CSVReader reader, List<LoadEntry> savedLoadEntryList) throws IOException {
        String[] nextLine;
        nextLine = reader.readNext();
        for (String string : nextLine) {
            try {
                savedLoadEntryList.add(LoadEntry.valueOf(string));
            } catch (Exception ignored) {
            }
        }
    }

    @VisibleForTesting
    String getSavedFileName(String input) {
        String fileName;
        try {
            if (input.length() == 0) return null;
            int index = Integer.valueOf(input);
            fileName = DIRECTORY_NAME + "/" + savedFileNameList.get(savedFileNameList.size() - index - 1);
        } catch (Exception e) {
            System.out.println(input + " is invalid.");
            return null;
        }
        return fileName;
    }

    public String showSavedFiles(Args args) {
        StringBuilder sb = new StringBuilder();

        savedFileNameList = savedFileNameList(args);
        int index = savedFileNameList.size() - 1;
        if (index >= 0) {
            for (String fileName : savedFileNameList) {
                String timestamp = parseTimestamp(args, fileName);

                try {
                    sb.append(index--).append(": ").append(DATE_FORMAT_PRINT.format(DATE_FORMAT_SAVE.parse(timestamp))).append("\n");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else {
            sb.append(NO_SAVED_FILE + "\n");
        }
        return sb.toString();
    }

    private String parseTimestamp(Args args, String fileName) {
        String prefix = prefix(args).toString();
        return fileName.replaceAll(prefix, "").replaceAll("[_\\.csv]", "");
    }

    @VisibleForTesting
    List<String> savedFileNameList(Args args) {
        List<String> fileNameList = new ArrayList<>();
        listFiles(fileNameList, args);
        Collections.sort(fileNameList);
        return fileNameList;
    }

    private void listFiles(List<String> fileNameList, Args args) {
        File[] files = new File(DIRECTORY_NAME).listFiles();
        if (files == null) return;

        for (final File file : files) {
            if (!file.isDirectory()) {
                String fileName = file.getName();
                if (fileName.startsWith(prefix(args).toString())) {
                    fileNameList.add(fileName);
                }
            }
        }
    }

    private enum Entry {header, body}
}
