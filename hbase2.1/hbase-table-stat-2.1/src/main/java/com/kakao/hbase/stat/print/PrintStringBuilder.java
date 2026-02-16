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

class PrintStringBuilder extends Builder {
    private static final int INDENT_SIZE = 2;

    private void appendValue(Length length, java.lang.StringBuilder sb, String value, Formatter.Type formatType) {
        sb.append(Color.leftPad(value, length.getValue(), PrintEntry.PADDING, formatType));
    }

    private void appendDiff(Length length, java.lang.StringBuilder sb, String diff, Formatter.Type formatType) {
        int padLen = length.getTotal() - length.getValue() - length.getDiff() - DELIMITER_DIFF.length() + INDENT_SIZE;
        sb.append(Color.leftPad(diff, length.getDiff(), PrintEntry.PADDING, formatType));
        for (int i = 0; i < padLen; i++) {
            sb.append(PrintEntry.PADDING);
        }
    }

    private void appendValueOnly(Length length, java.lang.StringBuilder sb, String value, Formatter.Type formatType) {
        sb.append(value);
        int padLen = length.getTotal() - Color.clearColor(value, formatType).length() + INDENT_SIZE;
        for (int i = 0; i < padLen; i++) {
            sb.append(PrintEntry.PADDING);
        }
    }

    @Override
    public void build(Map<String, Length> lengthMap, java.lang.StringBuilder sb, String key, String value, String diff, Formatter.Type formatType) {
        Length length = lengthMap.get(Color.clearColor(key, formatType));
        if (getLength(diff) > 0) {
            appendValue(length, sb, value, formatType);
            sb.append(DELIMITER_DIFF);
            appendDiff(length, sb, diff, formatType);
        } else {
            appendValueOnly(length, sb, value, formatType);
        }
    }
}
