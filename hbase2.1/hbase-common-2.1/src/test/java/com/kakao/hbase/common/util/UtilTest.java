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

package com.kakao.hbase.common.util;

import com.kakao.hbase.common.Constant;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class UtilTest {
    @Test
    public void testParseTimestamp() {
        try {
            Util.parseTimestamp("0");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains(Constant.MESSAGE_INVALID_DATE_FORMAT)) {
                throw e;
            }
        }

        try {
            Util.parseTimestamp("20200101000000");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains(Constant.MESSAGE_INVALID_DATE_FORMAT)) {
                throw e;
            }
        }

        Assert.assertEquals("20151130102200",
            Constant.DATE_FORMAT_ARGS.format(new Date(Util.parseTimestamp("20151130102200"))));
    }
}
