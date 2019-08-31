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

import java.util.Map;

class LengthMapBuilder extends Builder {
    @Override
    public void build(Map<String, Length> lengthMap, StringBuilder sb, String key, String value, String diff, Formatter.Type formatType) {
        Length len = Length.getLength(lengthMap, Color.clearColor(key, formatType));

        int valueLengthNow = Math.max(len.getValue(), Color.clearColor(value, formatType).length());
        int diffLengthNow = Math.max(len.getDiff(), Color.clearColor(diff, formatType).length());

        if (getLength(diff) > 0) {
            len.setValue(valueLengthNow);
            len.setDiff(diffLengthNow);

            int totalLengthNow = Math.max(len.getTotal(), (valueLengthNow + diffLengthNow + DELIMITER_DIFF.length()));
            len.setTotal(totalLengthNow);
        } else {
            int totalLengthNow = Math.max(len.getTotal(), (valueLengthNow + diffLengthNow));
            len.setTotal(totalLengthNow);
        }
    }
}
