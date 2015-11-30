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

package com.kakao.hbase.common;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;

public class Constant {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public static final long WAIT_INTERVAL_MS = 1000;
    public static final long LARGE_WAIT_INTERVAL_MS = 60000;
    public static final long SMALL_WAIT_INTERVAL_MS = 3000;
    public static final int TRY_MAX = 20;
    public static final String TABLE_DELIMITER = ",";
    public static final String MESSAGE_CANNOT_MOVE = "Can not move region";
    public static final String MESSAGE_DISABLED_OR_NOT_FOUND_TABLE = "Disabled or not found table";
    public static final String MESSAGE_NEED_REFRESH = "need refresh";
    public static final String UNIT_TEST_TABLE_PREFIX = "UNIT_TEST_";
    public static final String MESSAGE_INVALID_DATE_FORMAT = "Invalid date format";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT_ARGS = new SimpleDateFormat("yyyyMMddHHmmss");

    private Constant() {
    }
}
