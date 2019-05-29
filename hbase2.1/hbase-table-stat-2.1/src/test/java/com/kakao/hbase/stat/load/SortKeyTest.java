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

package com.kakao.hbase.stat.load;

import com.kakao.hbase.common.LoadEntry;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SortKeyTest {
    @Test
    public void testSort() {
        SortKey sortKey;

        try {
            new SortKey(null);
            fail();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            new SortKey("-");
            fail();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            new SortKey("1d");
            fail();
        } catch (IllegalArgumentException ignored) {
        }

        sortKey = new SortKey("0");
        assertEquals(SortKey.DEFAULT, sortKey);

        sortKey = new SortKey("1");
        Assert.assertEquals(LoadEntry.Reads, sortKey.getLoadEntry());
        assertEquals(SortKey.ValueEntry.value, sortKey.getValueEntry());

        sortKey = new SortKey("@");
        Assert.assertEquals(LoadEntry.Writes, sortKey.getLoadEntry());
        assertEquals(SortKey.ValueEntry.diff, sortKey.getValueEntry());
    }
}
