package com.kakao.hbase.stat.print;

import com.kakao.hbase.common.LoadEntry;
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
        String expected = "\u001B[32mRegionServer\u001B[0m  \u001B[36mReads\u001B[0m      \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m Total: 0\u001B[0m     N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";

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
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        loadMap.put(new Level("rec11"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Writes, 11223344556677L);
        loadMap.put(new Level("rec1122334455"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 11223344);
        summary.put(LoadEntry.Writes, 0);

        load.setLoadMap(loadMap);
        load.setSummary(summary);

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer    Reads      Writes                Regions    Files      FileSize   FileSizeUncomp  MemstoreSize     CompactedKVs  \n" +
                " rec11          N/A | N/A               0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A         112233m | N/A  N/A | N/A     \n" +
                " rec1122334455  N/A | N/A  11223344556677 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A             N/A | N/A  N/A | N/A     \n" +
                " Total: 2       N/A | N/A               0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       11223344m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 11223344);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize    CompactedKVs  \n" +
                " rec11        N/A | N/A  0 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A         112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  1 | 1   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       11223344m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 3
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 1122);
        rec1.put(LoadEntry.Writes, 0);
        loadMap.put(new Level("rec1122"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 11223344);
        summary.put(LoadEntry.Writes, 0);

        load.setLoadMap(loadMap);
        load.setSummary(summary);

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes   Regions    Files      FileSize   FileSizeUncomp  MemstoreSize     CompactedKVs  \n" +
                " rec1122      N/A | N/A  0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A           1122m | N/A  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  0 |  -1  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       11223344m |  0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }

    @Test
    public void testShowChangedOnly() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isShowChangedOnly());

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes   Regions    Files      FileSize   FileSizeUncomp  MemstoreSize   CompactedKVs  \n" +
                " rec11        N/A | N/A  1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isShowChangedOnly());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " rec11        N/A | N/A  1 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  1 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 3
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.toggleShowChangedOnly();
        load.updateChangeMap();
        Assert.assertTrue(load.isShowChangedOnly());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " Total: 1     N/A | N/A  1 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 4
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.toggleShowChangedOnly();
        load.updateChangeMap();
        Assert.assertFalse(load.isShowChangedOnly());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " rec11        N/A | N/A  1 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  1 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }

    @Test
    public void testDiffFromStart() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 0);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes   Regions    Files      FileSize   FileSizeUncomp  MemstoreSize   CompactedKVs  \n" +
                " rec11        N/A | N/A  0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " rec11        N/A | N/A  1 | 1   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  1 | 1   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 3
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 2);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.toggleDiffFromStart();
        load.updateChangeMap();
        Assert.assertTrue(load.isDiffFromStart());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " rec11        N/A | N/A  2 | 2   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  2 | 2   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 4
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 2);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.toggleDiffFromStart();
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " rec11        N/A | N/A  2 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  2 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }


    @Test
    public void testResetDiffStartPoint() throws Exception {
        String resultString;
        LoadRecord rec1;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));
        Formatter formatter = new Formatter("testTable", load);

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 0);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        load.toggleDiffFromStart();
        Assert.assertTrue(load.isDiffFromStart());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes   Regions    Files      FileSize   FileSizeUncomp  MemstoreSize   CompactedKVs  \n" +
                " rec11        N/A | N/A  0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  0 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertTrue(load.isDiffFromStart());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes  Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " rec11        N/A | N/A  1 | 1   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  1 | 1   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | 0m  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // resetDiffFromStart
        load.resetDiffStartPoint();
        Assert.assertEquals(Load.EMPTY_TIMESTAMP, load.getTimestampStart());

        // iteration 3
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 2);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertTrue(load.isDiffFromStart());

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes   Regions    Files      FileSize   FileSizeUncomp  MemstoreSize   CompactedKVs  \n" +
                " rec11        N/A | N/A  2 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  2 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }


    @Test
    public void testResetDiffStartPointWithShowRate() throws Exception {
        String resultString;
        LoadRecord rec1;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));
        Formatter formatter = new Formatter("testTable", load);

        // resetDiffFromStart and showRate
        load.resetDiffStartPoint();
        Assert.assertEquals(Load.EMPTY_TIMESTAMP, load.getTimestampStart());
        Assert.assertEquals(0, load.getTotalDuration());
        load.toggleShowRate();
        Assert.assertTrue(load.isShowRate());

        // iteration 1
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112234);
        rec1.put(LoadEntry.Writes, 3);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112234);
        summary.put(LoadEntry.Writes, 3);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        load.setDuration(0);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes   Regions    Files      FileSize   FileSizeUncomp  MemstoreSize   CompactedKVs  \n" +
                " rec11        N/A | N/A  3 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112234m | N/A  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  3 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112234m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112235);
        rec1.put(LoadEntry.Writes, 4);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112235);
        summary.put(LoadEntry.Writes, 4);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        load.setDuration(1000);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes     Regions    Files      FileSize   FileSizeUncomp  MemstoreSize      CompactedKVs  \n" +
                " rec11        N/A | N/A  4 | 1.0/s  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112235m | 1.0m/s  N/A | N/A     \n" +
                " Total: 1     N/A | N/A  4 | 1.0/s  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       112235m | 1.0m/s  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }

    @Test
    public void testShowRate() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 0);
        rec1.put(LoadEntry.Reads, 0);
        rec1.put(LoadEntry.FileSize, 0);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 0);
        summary.put(LoadEntry.Reads, 0);
        summary.put(LoadEntry.FileSize, 0);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        formatter = new Formatter("testTable", load);
        Assert.assertFalse(load.isShowRate());
        load.toggleShowRate();
        Assert.assertTrue(load.isShowRate());
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "RegionServer  Reads    Writes   Regions    Files      FileSize  FileSizeUncomp  MemstoreSize   CompactedKVs  \n" +
                " rec11        0 | N/A  0 | N/A  N/A | N/A  N/A | N/A  0m | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n" +
                " Total: 1     0 | N/A  0 | N/A  N/A | N/A  N/A | N/A  0m | N/A  N/A | N/A       112233m | N/A  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 112233);
        rec1.put(LoadEntry.Writes, 1000);
        rec1.put(LoadEntry.Reads, 1);
        rec1.put(LoadEntry.FileSize, 0);
        loadMap.put(new Level("rec11"), rec1);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 112233);
        summary.put(LoadEntry.Writes, 1000);
        summary.put(LoadEntry.Reads, 1);
        summary.put(LoadEntry.FileSize, 0);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        load.setDuration(500);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        Assert.assertTrue(load.isShowRate());
        System.out.println(resultString);
        expected = "RegionServer  Reads      Writes         Regions    Files      FileSize   FileSizeUncomp  MemstoreSize    CompactedKVs  \n" +
                " rec11        1 | 2.0/s  1000 | 2000/s  N/A | N/A  N/A | N/A  0m | 0m/s  N/A | N/A       112233m | 0m/s  N/A | N/A     \n" +
                " Total: 1     1 | 2.0/s  1000 | 2000/s  N/A | N/A  N/A | N/A  0m | 0m/s  N/A | N/A       112233m | 0m/s  N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }

    @Test
    public void testColoringChangedValue() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1, rec2;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 1);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 1);
        summary.put(LoadEntry.Writes, 3);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[32mRegionServer\u001B[0m  \u001B[36mReads\u001B[0m      \u001B[36mWrites\u001B[0m   \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec1\u001B[0m         N/A | N/A  1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m | N/A     N/A | N/A     \n" +
                "\u001B[1m rec2\u001B[0m         N/A | N/A  2 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     N/A | N/A  3 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 1);
        rec1.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 1);
        summary.put(LoadEntry.Writes, 4);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();

        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[32mRegionServer\u001B[0m  \u001B[36mReads\u001B[0m      \u001B[36mWrites\u001B[0m  \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec1\u001B[0m         N/A | N/A  \u001B[33m2\u001B[0m | \u001B[33m1\u001B[0m   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m |  0m     N/A | N/A     \n" +
                "\u001B[1m rec2\u001B[0m         N/A | N/A  2 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     N/A | N/A  \u001B[33m4\u001B[0m | \u001B[33m1\u001B[0m   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m |  0m     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);
    }

    @Test
    public void testSort() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1, rec2;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        Assert.assertEquals(SortKey.DEFAULT, load.getSortKey());

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[32mRegionServer\u001B[0m  \u001B[36mReads\u001B[0m    \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec1\u001B[0m         2 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m rec2\u001B[0m         1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     3 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 2
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3);

        load.prepare();
        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        load.setSortKey(new SortKey("1"));

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[36mRegionServer\u001B[0m  \u001B[32mReads\u001B[0m  \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec2\u001B[0m         1 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m rec1\u001B[0m         2 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     3 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 3
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3);

        load.prepare();
        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        load.setSortKey(new SortKey("!"));

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[36mRegionServer\u001B[0m  \u001B[42mReads\u001B[0m  \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec1\u001B[0m         2 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m rec2\u001B[0m         1 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     3 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 4. reset to 0
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3);

        load.prepare();
        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        load.setSortKey(SortKey.DEFAULT);
        Assert.assertEquals(SortKey.DEFAULT, load.getSortKey());

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[32mRegionServer\u001B[0m  \u001B[36mReads\u001B[0m  \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec1\u001B[0m         2 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m rec2\u001B[0m         1 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     3 | 0  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 5. if value is same then sorted by level string
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 3);

        load.prepare();
        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        load.setSortKey(new SortKey("!"));
        load.toggleShowRate();
        Assert.assertTrue(load.isShowRate());

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[36mRegionServer\u001B[0m  \u001B[42mReads\u001B[0m    \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec1\u001B[0m         2 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m rec2\u001B[0m         1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     3 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 6
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.Reads, 3);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Reads, 1);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.Reads, 4);

        load.prepare();
        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();
        load.setDuration(500);
        Assert.assertTrue(load.isShowRate());

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "\u001B[36mRegionServer\u001B[0m  \u001B[42mReads\u001B[0m      \u001B[36mWrites\u001B[0m     \u001B[36mRegions\u001B[0m    \u001B[36mFiles\u001B[0m      \u001B[36mFileSize\u001B[0m   \u001B[36mFileSizeUncomp\u001B[0m  \u001B[36mMemstoreSize\u001B[0m  \u001B[36mCompactedKVs\u001B[0m  \n" +
                "\u001B[1m rec2\u001B[0m         1 |   0/s  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m rec1\u001B[0m         \u001B[33m3\u001B[0m | \u001B[33m2.0/s\u001B[0m  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "\u001B[1m Total: 2\u001B[0m     \u001B[33m4\u001B[0m | \u001B[33m2.0/s\u001B[0m  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);
    }

    @Test
    public void testRegion() throws Exception {
        Formatter formatter;
        String resultString;
        String expected;
        Load load = new Load(new LevelClass(RegionName.class));

        load.updateChangeMap();
        Assert.assertFalse(load.isDiffFromStart());

        formatter = new Formatter("testTable", load);
        Assert.assertFalse(load.isShowRate());
        load.toggleShowRate();
        Assert.assertTrue(load.isShowRate());
        resultString = formatter.buildString(false, Formatter.Type.ANSI);
        System.out.println(resultString);
        expected = "Region (RS Index)  Reads      Writes     Regions    Files      FileSize   FileSizeUncomp  MemstoreSize  CompactedKVs  \n" +
                " Total: 0          N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, Color.clearColor(resultString, Formatter.Type.ANSI));
    }

    @Test
    public void testHTML() throws Exception {
        Formatter formatter;
        String resultString;
        LoadRecord rec1, rec2;
        String expected;
        Map<Level, LoadRecord> loadMap;
        LoadRecord summary;
        Load load = new Load(new LevelClass(ServerName.class));

        // iteration 1
        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 1);
        rec1.put(LoadEntry.Writes, 1);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 1);
        summary.put(LoadEntry.Writes, 3);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();

        formatter = new Formatter("testTable", load);
        resultString = formatter.buildString(false, Formatter.Type.HTML);
        System.out.println(resultString);
        expected = "<span style=\"color:green\">RegionServer</span>  <span style=\"color:cyan\">Reads</span>      <span style=\"color:cyan\">Writes</span>   <span style=\"color:cyan\">Regions</span>    <span style=\"color:cyan\">Files</span>      <span style=\"color:cyan\">FileSize</span>   <span style=\"color:cyan\">FileSizeUncomp</span>  <span style=\"color:cyan\">MemstoreSize</span>  <span style=\"color:cyan\">CompactedKVs</span>  \n" +
                "<b> rec1</b>         N/A | N/A  1 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m | N/A     N/A | N/A     \n" +
                "<b> rec2</b>         N/A | N/A  2 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "<b> Total: 2</b>     N/A | N/A  3 | N/A  N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m | N/A     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);

        // iteration 2
        load.prepare();

        loadMap = new TreeMap<>();
        rec1 = new LoadRecord();
        rec1.put(LoadEntry.MemstoreSize, 1);
        rec1.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec1"), rec1);
        rec2 = new LoadRecord();
        rec2.put(LoadEntry.Writes, 2);
        loadMap.put(new Level("rec2"), rec2);

        summary = new LoadRecord();
        summary.put(LoadEntry.MemstoreSize, 1);
        summary.put(LoadEntry.Writes, 4);

        load.setLoadMap(loadMap);
        load.setSummary(summary);
        load.updateChangeMap();

        resultString = formatter.buildString(false, Formatter.Type.HTML);
        System.out.println(resultString);
        expected = "<span style=\"color:green\">RegionServer</span>  <span style=\"color:cyan\">Reads</span>      <span style=\"color:cyan\">Writes</span>  <span style=\"color:cyan\">Regions</span>    <span style=\"color:cyan\">Files</span>      <span style=\"color:cyan\">FileSize</span>   <span style=\"color:cyan\">FileSizeUncomp</span>  <span style=\"color:cyan\">MemstoreSize</span>  <span style=\"color:cyan\">CompactedKVs</span>  \n" +
                "<b> rec1</b>         N/A | N/A  <span style=\"color:yellow\">2</span> | <span style=\"color:yellow\">1</span>   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m |  0m     N/A | N/A     \n" +
                "<b> rec2</b>         N/A | N/A  2 | 0   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A       N/A | N/A     N/A | N/A     \n" +
                "<b> Total: 2</b>     N/A | N/A  <span style=\"color:yellow\">4</span> | <span style=\"color:yellow\">1</span>   N/A | N/A  N/A | N/A  N/A | N/A  N/A | N/A        1m |  0m     N/A | N/A     \n";
        Assert.assertEquals(expected, resultString);
    }
}
