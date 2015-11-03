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
import com.kakao.hbase.common.RatioNumber;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.stat.StatArgs;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class LoadIOTest {
    @Test
    public void testSaveOutput() throws Exception {
        Load load = new Load(new LevelClass(TableName.class));
        LoadIO loadIO = createLoadIO(load);

        String outputFileName = "output.txt";
        String[] argsRaw = {"zookeeper", "table", "--output=" + outputFileName};
        Args args = new StatArgs(argsRaw);

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
            assertEquals("\"Table\",\"Timestamp\",\"Reads\",\"Writes\",\"Regions\",\"Files\",\"FileSize\",\"FileSizeUncomp\",\"DataLocality\",\"MemstoreSize\",\"CompactedKVs\"", strings.get(0));
            assertEquals("\"rec1\",\"" + timestamp1 + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"0.0:0.0\",\"\",\"\"", strings.get(1));
            assertEquals("\"rec2\",\"" + timestamp1 + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"0.0:1.0\",\"\",\"\"", strings.get(2));

            // iteration 2
            Thread.sleep(1);
            load.update(null, args);
            strings = Files.readAllLines(Paths.get(outputFileName), Charset.defaultCharset());
            for (String string : strings) {
                System.out.println(string);
            }
            assertEquals(5, strings.size());
            long timestamp2 = load.getTimestampIteration();
            assertEquals("\"Table\",\"Timestamp\",\"Reads\",\"Writes\",\"Regions\",\"Files\",\"FileSize\",\"FileSizeUncomp\",\"DataLocality\",\"MemstoreSize\",\"CompactedKVs\"", strings.get(0));
            assertEquals("\"rec1\",\"" + timestamp1 + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"0.0:0.0\",\"\",\"\"", strings.get(1));
            assertEquals("\"rec2\",\"" + timestamp1 + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"0.0:1.0\",\"\",\"\"", strings.get(2));
            assertEquals("\"rec1\",\"" + timestamp2 + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"0.0:0.0\",\"\",\"\"", strings.get(3));
            assertEquals("\"rec2\",\"" + timestamp2 + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"0.0:1.0\",\"\",\"\"", strings.get(4));

            assertEquals(0, loadIO.savedFileNameList(args).size());
        } finally {
            Files.delete(Paths.get(outputFileName));
        }
    }

    @Test
    public void testSave() throws Exception {
        Load load = new Load(new LevelClass(TableName.class));
        LoadIO loadIO = createLoadIO(load);

        String filename;
        String[] argsRaw = {"zookeeper", "table"};
        Args args = new StatArgs(argsRaw);
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
            assertEquals("\"Table\",\"Timestamp\",\"Reads\",\"Writes\",\"Regions\",\"Files\",\"FileSize\",\"FileSizeUncomp\",\"DataLocality\",\"MemstoreSize\",\"CompactedKVs\"", strings.get(0));
            assertEquals("\"rec1\",\"" + timestamp + "\",\"2\",\"\",\"\",\"\",\"\",\"\",\"0.0:0.0\",\"\",\"\"", strings.get(1));
            assertEquals("\"rec2\",\"" + timestamp + "\",\"1\",\"\",\"\",\"\",\"\",\"\",\"0.0:1.0\",\"\",\"\"", strings.get(2));

            assertEquals(1, loadIO.savedFileNameList(args).size());
        } finally {
            Files.delete(Paths.get(filename));
        }
    }

    @Test
    public void testLoad() throws Exception {
        Load load = new Load(new LevelClass(TableName.class));
        LoadIO loadIO = createLoadIO(load);

        String filename = null;
        String[] argsRaw = {"zookeeper", "table"};
        Args args = new StatArgs(argsRaw);

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

    private LoadIO createLoadIO(Load load) {
        LoadRecord rec1, rec2;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        LoadIO loadIO = new LoadIO(load);

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2L);
        rec1.put(LoadEntry.DataLocality, new RatioNumber(0, 0));
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1L);
        rec2.put(LoadEntry.DataLocality, new RatioNumber(0, 1));
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3L);
        summary.put(LoadEntry.DataLocality, new RatioNumber(1, 1));

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        return loadIO;
    }
}
