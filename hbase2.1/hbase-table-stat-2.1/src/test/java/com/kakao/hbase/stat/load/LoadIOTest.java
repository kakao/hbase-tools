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
import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.stat.StatArgs;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class LoadIOTest {
    @Test
    public void testFilename() throws Exception {
        String[] strings;
        Args args;
        Load load = new Load(new LevelClass(TableName.class), null);
        LoadIO loadIO = new LoadIO(load);

        strings = new String[]{"zookeeper", "table"};
        args = new StatArgs(strings);
        assertEquals(LoadIO.DIRECTORY_NAME + "/zookeeper_table_19700101090000000.csv", loadIO.filename(args));

        strings = new String[]{"zookeeper"};
        args = new StatArgs(strings);
        assertEquals(LoadIO.DIRECTORY_NAME + "/zookeeper__19700101090000000.csv", loadIO.filename(args));

        strings = new String[]{"zookeeper1:2180,zookeeper2:2180"};
        args = new StatArgs(strings);
        assertEquals(LoadIO.DIRECTORY_NAME + "/zookeeper1_2180_zookeeper2_2180__19700101090000000.csv", loadIO.filename(args));
    }

    @Test
    public void testSaveOutput() throws Exception {
        String outputFileName = "output.txt";
        String[] argsRaw = {"zookeeper", "table", "--output=" + outputFileName};
        Args args = new StatArgs(argsRaw);

        Load load = new Load(new LevelClass(TableName.class), args);
        LoadIO loadIO = createLoadIO(load);

        try {
            Files.delete(Paths.get(outputFileName));
        } catch (NoSuchFileException e) {
            if (!e.getMessage().contains(outputFileName)) throw e;
        }

        List<String> strings;
        try {
            // iteration 1
            load.update(null, args);
            strings = Files.readAllLines(Paths.get(outputFileName), Charset.defaultCharset());
            for (String string : strings) {
                System.out.println(string);
            }
            long timestamp1 = load.getTimestampIteration();
            assertEquals(3, strings.size());
            assertEquals("\"Table\",\"Timestamp\",\"Reads\",\"Writes\",\"Regions\",\"Files\",\"FileSize\",\"FileSizeUncomp\",\"MemstoreSize\",\"CompactedKVs\"", strings.get(0));
            assertEquals("\"rec1\",\"" + timestamp1 + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(1));
            assertEquals("\"rec2\",\"" + timestamp1 + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(2));

            // iteration 2
            Thread.sleep(1);
            load.update(null, args);
            strings = Files.readAllLines(Paths.get(outputFileName), Charset.defaultCharset());
            for (String string : strings) {
                System.out.println(string);
            }
            assertEquals(5, strings.size());
            long timestamp2 = load.getTimestampIteration();
            assertEquals("\"Table\",\"Timestamp\",\"Reads\",\"Writes\",\"Regions\",\"Files\",\"FileSize\",\"FileSizeUncomp\",\"MemstoreSize\",\"CompactedKVs\"", strings.get(0));
            assertEquals("\"rec1\",\"" + timestamp1 + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(1));
            assertEquals("\"rec2\",\"" + timestamp1 + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(2));
            assertEquals("\"rec1\",\"" + timestamp2 + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(3));
            assertEquals("\"rec2\",\"" + timestamp2 + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(4));

            assertEquals(0, loadIO.savedFileNameList(args).size());
        } finally {
            Files.delete(Paths.get(outputFileName));
        }
    }

    @Test
    public void testSave() throws Exception {
        String filename;
        String[] argsRaw = {"zookeeper", "table"};
        Args args = new StatArgs(argsRaw);

        Load load = new Load(new LevelClass(TableName.class), args);
        LoadIO loadIO = createLoadIO(load);

        filename = loadIO.filename(args);
        long timestamp = loadIO.getTimestamp(args, filename);

        try {
            assertEquals(0, loadIO.savedFileNameList(args).size());

            loadIO.save(args);
            List<String> strings = Files.readAllLines(Paths.get(filename), Charset.defaultCharset());
            for (String string : strings) {
                System.out.println(string);
            }
            assertEquals(3, strings.size());
            assertEquals("\"Table\",\"Timestamp\",\"Reads\",\"Writes\",\"Regions\",\"Files\",\"FileSize\",\"FileSizeUncomp\",\"MemstoreSize\",\"CompactedKVs\"", strings.get(0));
            assertEquals("\"rec1\",\"" + timestamp + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(1));
            assertEquals("\"rec2\",\"" + timestamp + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"", strings.get(2));

            assertEquals(1, loadIO.savedFileNameList(args).size());
        } finally {
            Files.delete(Paths.get(filename));
        }
    }

    @Test
    public void testFileNameList() throws Exception {
        String[] argsRaw1 = {"zookeeper", "table1"};
        Args args1 = new StatArgs(argsRaw1);

        Load load = new Load(new LevelClass(TableName.class), args1);
        LoadIO loadIO = createLoadIO(load);

        String[] argsRaw2 = {"zookeeper", "table2"};
        Args args2 = new StatArgs(argsRaw2);
        String filename1 = null;
        String filename2 = null;
        String filename3 = null;
        String filename4 = null;
        try {
            assertEquals(0, loadIO.savedFileNameList(args1).size());

            filename1 = loadIO.filename(args1);
            loadIO.save(args1);
            Thread.sleep(1);
            load.update(null, args1);
            assertEquals(1, loadIO.savedFileNameList(args1).size());

            filename2 = loadIO.filename(args1);
            loadIO.save(args1);
            Thread.sleep(1);
            load.update(null, args1);
            assertEquals(2, loadIO.savedFileNameList(args1).size());
            assertEquals(filename1, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(0));
            assertEquals(filename2, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(1));

            filename3 = loadIO.filename(args1);
            loadIO.save(args1);
            Thread.sleep(1);
            load.update(null, args1);
            assertEquals(3, loadIO.savedFileNameList(args1).size());
            assertEquals(filename1, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(0));
            assertEquals(filename2, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(1));
            assertEquals(filename3, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(2));

            filename4 = loadIO.filename(args2);
            loadIO.save(args2);
            loadIO.showSavedFiles(args1);
            assertEquals(3, loadIO.savedFileNameList(args1).size());
            assertEquals(filename1, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(0));
            assertEquals(filename2, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(1));
            assertEquals(filename3, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args1).get(2));
            assertEquals(filename3, loadIO.getSavedFileName("0"));
            assertEquals(filename2, loadIO.getSavedFileName("1"));
            assertEquals(filename1, loadIO.getSavedFileName("2"));
            assertNull(loadIO.getSavedFileName("3"));
            assertNull(loadIO.getSavedFileName("aa"));
            assertEquals(1, loadIO.savedFileNameList(args2).size());
            loadIO.showSavedFiles(args2);
            assertEquals(filename4, loadIO.getSavedFileName("0"));
            assertEquals(filename4, LoadIO.DIRECTORY_NAME + "/" + loadIO.savedFileNameList(args2).get(0));
        } finally {
            if (filename1 != null) Files.delete(Paths.get(filename1));
            if (filename2 != null) Files.delete(Paths.get(filename2));
            if (filename3 != null) Files.delete(Paths.get(filename3));
            if (filename4 != null) Files.delete(Paths.get(filename4));
        }
    }

    @Test
    public void testLoad() throws Exception {
        String filename = null;
        String[] argsRaw = {"zookeeper", "table"};
        Args args = new StatArgs(argsRaw);

        Load load = new Load(new LevelClass(TableName.class), args);
        LoadIO loadIO = createLoadIO(load);

        try {
            assertTrue(loadIO.showSavedFiles(args).startsWith(LoadIO.NO_SAVED_FILE));

            filename = loadIO.filename(args);
            loadIO.save(args);
            assertEquals(1, loadIO.savedFileNameList(args).size());
            List<String> strings = Files.readAllLines(Paths.get(filename), Charset.defaultCharset());
            for (String string : strings) {
                System.out.println(string);
            }

            assertFalse(loadIO.showSavedFiles(args).startsWith(LoadIO.NO_SAVED_FILE));
            loadIO.load(args, "0");
            assertEquals(filename, loadIO.getSavedFileName("0"));

            load.toggleDiffFromStart();
            assertTrue(load.isDiffFromStart());
            assertEquals(2, load.getLoadMap().size());
            assertEquals(2L, ((LoadRecord) (load.getLoadMapPrev().values().toArray()[0])).get(LoadEntry.Reads));
            assertEquals(1L, ((LoadRecord) (load.getLoadMapPrev().values().toArray()[1])).get(LoadEntry.Reads));
        } finally {
            if (filename != null) Files.delete(Paths.get(filename));
        }
    }

    @Test
    public void testNoDirectory() throws Exception {
        String[] argsRaw = {"zookeeper", "table"};
        Args args = new StatArgs(argsRaw);

        Load load = new Load(new LevelClass(TableName.class), args);
        LoadIO loadIO = createLoadIO(load);

        try {
            Files.delete(Paths.get(LoadIO.DIRECTORY_NAME));
        } catch (NoSuchFileException ignored) {
        }
        assertTrue(loadIO.showSavedFiles(args).startsWith(LoadIO.NO_SAVED_FILE));
    }

    @Test
    public void testInvalidLoad() throws Exception {
        String filename;
        String[] argsRaw = {"zookeeper", "table"};
        Args args = new StatArgs(argsRaw);

        Load load = new Load(new LevelClass(TableName.class), args);
        LoadIO loadIO = createLoadIO(load);

        filename = loadIO.filename(args);

        try {
            assertTrue(loadIO.showSavedFiles(args).startsWith(LoadIO.NO_SAVED_FILE));

            loadIO.save(args);
            assertEquals(1, loadIO.savedFileNameList(args).size());
            List<String> strings = Files.readAllLines(Paths.get(filename), Charset.defaultCharset());
            for (String string : strings) {
                System.out.println(string);
            }

            assertFalse(loadIO.showSavedFiles(args).startsWith(LoadIO.NO_SAVED_FILE));
            loadIO.load(args, "1");
            loadIO.load(args, "a");
        } finally {
            Files.delete(Paths.get(filename));
        }
    }

    @Test
    public void testTimestamp() throws Exception {
        Date nowDate = new Date();
        long now = nowDate.getTime();
        String nowString = LoadIO.DATE_FORMAT_SAVE.format(now);
        System.out.println(nowString);
        Date parsed = LoadIO.DATE_FORMAT_SAVE.parse(nowString);
        long nowParsed = parsed.getTime();
        assertEquals(now, nowParsed);
    }

    private LoadIO createLoadIO(Load load) {
        LoadRecord rec1, rec2;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        LoadIO loadIO = new LoadIO(load);

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2L);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1L);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3L);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        return loadIO;
    }
}
