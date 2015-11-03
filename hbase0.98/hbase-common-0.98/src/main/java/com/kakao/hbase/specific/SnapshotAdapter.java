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

package com.kakao.hbase.specific;

import com.kakao.hbase.common.Args;
import joptsimple.OptionSet;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * For HBase 0.98/1.0
 */
public class SnapshotAdapter {
    public static HBaseProtos.SnapshotDescription.Type getType(String tableName, Map<String, Boolean> tableFlushMap) {
        if (tableFlushMap.get(tableName)) {
            return HBaseProtos.SnapshotDescription.Type.SKIPFLUSH;
        } else {
            return HBaseProtos.SnapshotDescription.Type.FLUSH;
        }
    }

    public static HBaseProtos.SnapshotDescription.Type getType(OptionSet optionSet) {
        if (optionSet.has(Args.OPTION_SKIP_FLUSH)) {
            return HBaseProtos.SnapshotDescription.Type.SKIPFLUSH;
        } else {
            return HBaseProtos.SnapshotDescription.Type.FLUSH;
        }
    }

    public static List<HBaseProtos.SnapshotDescription> getSnapshotDescriptions(HBaseAdmin admin, String targetSnapshotName) throws IOException {
        return admin.listSnapshots(targetSnapshotName);
    }
}
