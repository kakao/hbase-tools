package com.kakao.hbase.snapshot;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

public class SnapshotUtilTest {
    @Test
    public void testGetSnapshotTimestamp() throws ParseException {
        String timestampStr = "20180309110025";
        long timestampMS = 1520560825000L;
        long snapshotTimestamp = SnapshotUtil.getSnapshotTimestamp("table1_S" + timestampStr);
        Date date = Snapshot.DATE_FORMAT_SNAPSHOT.parse(timestampStr);
        Assert.assertEquals(date.getTime(), snapshotTimestamp);
        Assert.assertEquals(timestampMS, snapshotTimestamp);
    }
}
