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

package com.kakao.hbase.stat.print;

import com.kakao.hbase.common.LoadEntry;
import com.kakao.hbase.common.RatioNumber;
import com.kakao.hbase.stat.load.*;
import org.apache.hadoop.hbase.ServerName;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

public class FormatterTest {
    @Test
    public void testEmptyLoad() throws Exception {
        Formatter formatter;
        String resultString;
        String expected = "\u001B[32mRegionServer\u001B[0m  \u001B[36mReads\u001B[0m      \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mDataLocality\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m Total: 0\u001B[0m     N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     N/A | N/A     \n";

        Load load = new Load(new LevelClass(ServerName.class));

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        Assert.assertEquals(expected, resultString);

        load.prepare();
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        Assert.assertEquals(expected, resultString);
    }

    @Test
    public void testSingleLoad() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1, rec2;
        String expected;
        Map<Level, LoadRecord> loadMap;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        rec1.put(LoadEntry.DataLocality, new RatioNumber(1, 0.5));
        loadMap.put(new Level("rec11"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Writes, 11223344556677L);
        rec2.put(LoadEntry.DataLocality, RatioNumber.ZERO);
        loadMap.put(new Level("rec1122334455"), rec2);

        load.setLoadMap(loadMap);
        load.summary(LoadEntry.MemstoreSize, 112233);
        load.summary(LoadEntry.Writes, 0);
        load.summary(LoadEntry.Writes, 11223344556677L);
        load.summary(LoadEntry.DataLocality, new RatioNumber(1, 0.5));
        load.summary(LoadEntry.DataLocality, RatioNumber.ZERO);

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer    Reads      Writes                Regions    Files      FileSize   FileSizeUncomp  DataLocality  MemstoreSize   CompactedKVs  \n" +
                " rec11          N/A | N/A               0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       50.00% | N/A  112233m | N/A  N/A | N/A     \n" +
                " rec1122334455  N/A | N/A  11223344556677 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        0.00% | N/A      N/A | N/A  N/A | N/A     \n" +
                " Total: 2       N/A | N/A  11223344556677 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       50.00% | N/A  112233m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        loadMap.put(new Level("rec11"), rec1);

        load.setLoadMap(loadMap);
        load.summary(LoadEntry.MemstoreSize, 112233);
        load.summary(LoadEntry.Writes, 0);
        load.summary(LoadEntry.DataLocality, new RatioNumber(2, 1.0));

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes               Regions    Files      FileSize   FileSizeUncomp  DataLocality       MemstoreSize  CompactedKVs  \n" +
            " rec11        N/A | N/A  0 |               0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A           N/A | -50.00%  112233m | 0m  N/A | N/A     \n" +
            " Total: 1     N/A | N/A  0 | -11223344556677  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       100.00% |  50.00%  112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }

    @Test
    public void testShowRate() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1;
        String expected;
        Map<Level, LoadRecord> loadMap;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.DataLocality, new RatioNumber(1, 0.5));
        rec1.put(LoadEntry.Regions, 1);
        loadMap.put(new Level("rec11"), rec1);

        load.setLoadMap(loadMap);
        load.summary(LoadEntry.DataLocality, new RatioNumber(1, 0.5));
        load.summary(LoadEntry.Regions, 1);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        formatter = new Formatter("testTable", load);
        Assert.assertFalse(load.isShowRate());
        load.toggleShowRate();
        Assert.assertTrue(load.isShowRate());
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes     Regions  Files      FileSize   FileSizeUncomp  DataLocality  MemstoreSize  CompactedKVs  \n" +
            " rec11        N/A | N/A  N/A | N/A  1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A       50.00% | N/A  N/A | N/A     N/A | N/A     \n" +
            " Total: 1     N/A | N/A  N/A | N/A  1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A       50.00% | N/A  N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.DataLocality, new RatioNumber(1, 0.5));
        rec1.put(LoadEntry.Regions, 1);
        loadMap.put(new Level("rec11"), rec1);

        load.setLoadMap(loadMap);
        load.summary(LoadEntry.DataLocality, new RatioNumber(2, 1.0));
        load.summary(LoadEntry.Regions, 2);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        load.setDuration(1000);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        Assert.assertTrue(load.isShowRate());
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes     Regions    Files      FileSize   FileSizeUncomp  DataLocality        MemstoreSize  CompactedKVs  \n" +
            " rec11        N/A | N/A  N/A | N/A  1 |   0/s  N/A | N/A  N/A | N/A  N/A | N/A        50.00% |  0.00%/s  N/A | N/A     N/A | N/A     \n" +
            " Total: 1     N/A | N/A  N/A | N/A  2 | 1.0/s  N/A | N/A  N/A | N/A  N/A | N/A       100.00% | 50.00%/s  N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }
}
