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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RatioNumberTest {
    @Test
    public void testIntValue() {
        Assert.assertEquals(1, new RatioNumber(1.0, 1.0).intValue());
        assertEquals(0, new RatioNumber(1.0, 0.1).intValue());
        assertEquals(0, new RatioNumber(1.0, 0.0).intValue());
        assertEquals(0, new RatioNumber(1.0, 0.5).intValue());
        assertEquals(1, new RatioNumber(1.0, 1.1).intValue());
        assertEquals(1, new RatioNumber(0.0, 1.1).intValue());
    }

    @Test
    public void testLongValue() {
        assertEquals(1L, new RatioNumber(1.0, 1.0).longValue());
        assertEquals(0L, new RatioNumber(1.0, 0.1).longValue());
        assertEquals(0L, new RatioNumber(1.0, 0.0).longValue());
        assertEquals(0L, new RatioNumber(1.0, 0.5).longValue());
        assertEquals(1L, new RatioNumber(1.0, 1.1).longValue());
        assertEquals(1L, new RatioNumber(0.0, 1.0).longValue());
        assertEquals(11L, new RatioNumber(1.0, 11).longValue());
    }

    @Test
    public void testFloatValue() {
        assertEquals(1.0f, new RatioNumber(1.0, 1.0).floatValue(), 0.0f);
        assertEquals(0.1f, new RatioNumber(1.0, 0.1).floatValue(), 0.0f);
        assertEquals(1.0f, new RatioNumber(0.0, 1.0).floatValue(), 0.0f);
        assertEquals(0.5f, new RatioNumber(1, 0.5).floatValue(), 0.0f);
        assertEquals(1.1f, new RatioNumber(1, 1.1).floatValue(), 0.0f);
        assertEquals(0.0f, new RatioNumber(11, 0.0).floatValue(), 0.0f);
    }

    @Test
    public void testDoubleValue() {
        assertEquals(0.1, new RatioNumber(10, 0.1).doubleValue(), 0.0);
        assertEquals(0.1, new RatioNumber(1, 0.1).doubleValue(), 0.0);
        assertEquals(0.0, new RatioNumber(10, 0).doubleValue(), 0.0);
        assertEquals(0.5, new RatioNumber(1, 0.5).doubleValue(), 0.0);
        assertEquals(0.1, new RatioNumber(11, 0.1).doubleValue(), 0.0);
        assertEquals(1.0, new RatioNumber(0, 1.0).doubleValue(), 0.0);
    }

    @Test
    public void testAdd() {
        RatioNumber r1;
        RatioNumber r2;

        r1 = new RatioNumber(10, 0.1);
        r2 = new RatioNumber(40, 0.1);
        assertEquals(50, r1.add(r2).value(), 0);
        assertEquals(0.1, r1.add(r2).doubleValue(), 0);

        r1 = new RatioNumber(40, 0.1);
        r2 = new RatioNumber(60, 0.1);
        assertEquals(100, r1.add(r2).value(), 0);
        assertEquals(0.1, r1.add(r2).doubleValue(), 0);

        r1 = new RatioNumber(40, 1);
        r2 = new RatioNumber(60, 1);
        assertEquals(100, r1.add(r2).value(), 0);
        assertEquals(1, r1.add(r2).doubleValue(), 0);

        r1 = new RatioNumber(40, 0.2);
        r2 = new RatioNumber(60, 0.1);
        assertEquals(100, r1.add(r2).value(), 0);
        assertEquals(0.14, r1.add(r2).doubleValue(), 0);

        r1 = new RatioNumber(1, 0.5);
        r2 = RatioNumber.ZERO;
        assertEquals(1.0, r1.add(r2).value(), 0);
        assertEquals(0.5, r1.add(r2).doubleValue(), 0);

        r1 = RatioNumber.ZERO;
        r2 = new RatioNumber(1, 0.5);
        assertEquals(1.0, r1.add(r2).value(), 0);
        assertEquals(0.5, r1.add(r2).doubleValue(), 0);

        r1 = RatioNumber.ZERO;
        r2 = new RatioNumber(0, 1.0);
        assertEquals(0, r1.add(r2).value(), 0);
        assertEquals(1.0, r1.add(r2).doubleValue(), 0);
    }

    @Test
    public void testAddZero() {
        Number ratioNumber = new RatioNumber(10, 0.1);
        Number zero = 0;

        assertEquals(ratioNumber, LoadEntry.DataLocality.add(ratioNumber, zero));
        assertEquals(ratioNumber, LoadEntry.DataLocality.add(zero, ratioNumber));
        assertEquals(zero, LoadEntry.DataLocality.add(zero, zero));
    }

    @Test
    public void testValueOf() {
        assertEquals(new RatioNumber(3, 4), RatioNumber.valueOf("3:4"));
        assertEquals(new RatioNumber(0, 0), RatioNumber.valueOf("0:0"));
    }

    @Test
    public void testToString() {
        assertEquals("0.0:0.0", RatioNumber.ZERO.toString());
        assertEquals("3.0:4.0", new RatioNumber(3, 4).toString());
    }
}
