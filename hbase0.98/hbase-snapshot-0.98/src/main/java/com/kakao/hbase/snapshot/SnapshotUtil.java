package com.kakao.hbase.snapshot;

public class SnapshotUtil {
    static long getSnapshotTimestamp(String snapshotName) {
        try {
            String suffix = snapshotName.substring(snapshotName.length() - 16);
            if (suffix.startsWith(Snapshot.TIMESTAMP_PREFIX)) {
                return Snapshot.DATE_FORMAT_SNAPSHOT.parse(suffix.substring(2)).getTime();
            } else {
                // invalid snapshot name format
                return 0;
            }
        } catch (Throwable e) {
            // invalid snapshot name format
            return 0;
        }
    }

    static boolean isOldZnode(String snapshotName) {
        long abortZnodeAge = System.currentTimeMillis() - getSnapshotTimestamp(snapshotName);
        return getSnapshotTimestamp(snapshotName) > 0 && abortZnodeAge > Snapshot.ABORT_ZNODE_AGE_THRESHOLD_MS;
    }
}
