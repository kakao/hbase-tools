package com.kakao.hbase.snapshot;

public class SnapshotUtil {
    static long getSnapshotTimestamp(String snapshotName) {
        try {
            return Snapshot.DATE_FORMAT_SNAPSHOT.parse(snapshotName.substring(snapshotName.length() - 14)).getTime();
        } catch (Throwable e) {
            return 0;
        }
    }
}
