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

public class LoadEntryTest {
    @Test
    public void testDataLocalityToString() {
        Assert.assertEquals("100.00%", LoadEntry.DataLocality.toString(1.00001));
        Assert.assertEquals("-0.00%", LoadEntry.DataLocality.toString(-0.000001));
        Assert.assertEquals("-0.01%", LoadEntry.DataLocality.toString(-0.00005));
        Assert.assertEquals("0.01%", LoadEntry.DataLocality.toString(0.00005));
        Assert.assertEquals("100.00%", LoadEntry.DataLocality.toString(0.999999));
    }

    @Test
    public void testDataLocalityToRateString() {
        Assert.assertEquals("5.00%/s", LoadEntry.DataLocality.toRateString(0.05, 1000));
        Assert.assertEquals("2.50%/s", LoadEntry.DataLocality.toRateString(0.05, 2000));
        Assert.assertEquals("0.03%/s", LoadEntry.DataLocality.toRateString(0.0005, 2000));
    }

    @Test
    public void testCompareAndCompareTo() {
        RatioNumber number1 = new RatioNumber(100, 0.1);
        RatioNumber number2 = new RatioNumber(100, 0.2);
        RatioNumber number3 = new RatioNumber(100, 0.1);
        Double number4 = 1.0;

        Assert.assertEquals(-1, LoadEntry.DataLocality.compare(number1, number2));
        Assert.assertEquals(1, LoadEntry.DataLocality.compare(number2, number1));
        Assert.assertEquals(0, LoadEntry.DataLocality.compare(number1, number3));
        Assert.assertEquals(-1, LoadEntry.DataLocality.compare(number1, number4));
        Assert.assertEquals(1, LoadEntry.DataLocality.compare(number4, number1));

        Assert.assertEquals(-1, number1.compareTo(number2));
        Assert.assertEquals(1, number2.compareTo(number1));
        Assert.assertEquals(0, number1.compareTo(number3));
    }

    @Test
    public void testCompareTo() {
        RatioNumber number1 = new RatioNumber(100, 0.1);
        RatioNumber number2 = new RatioNumber(100, 0.2);
        RatioNumber number3 = new RatioNumber(100, 0.1);

        Assert.assertEquals(-1, number1.compareTo(number2));
        Assert.assertEquals(1, number2.compareTo(number1));
        Assert.assertEquals(0, number1.compareTo(number3));
    }
}
