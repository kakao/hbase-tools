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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DecimalStringSplitTest {
    @Test
    public void testSplit10_10() {
        int numRegions = 10;
        int cardinality = 10;

        RegionSplitter.SplitAlgorithm splitAlgorithm = new DecimalStringSplit(cardinality);
        byte[][] splits = splitAlgorithm.split(numRegions);
        assertEquals(numRegions - 1, splits.length);

        int digits = 2;
        assertEquals(String.format("%0" + digits + "d", 1), Bytes.toString(splits[0]));
        assertEquals(String.format("%0" + digits + "d", 9), Bytes.toString(splits[numRegions - 2]));
    }

    @Test
    public void testSplit3_10() {
        int numRegions = 3;
        int cardinality = 10;

        RegionSplitter.SplitAlgorithm splitAlgorithm = new DecimalStringSplit(cardinality);
        byte[][] splits = splitAlgorithm.split(numRegions);
        assertEquals(numRegions - 1, splits.length);

        int digits = 2;
        assertEquals(String.format("%0" + digits + "d", 3), Bytes.toString(splits[0]));
        assertEquals(String.format("%0" + digits + "d", 6), Bytes.toString(splits[numRegions - 2]));
    }


    @Test
    public void testSplit300_1000() {
        int numRegions = 300;
        int cardinality = 1000;

        RegionSplitter.SplitAlgorithm splitAlgorithm = new DecimalStringSplit(cardinality);
        byte[][] splits = splitAlgorithm.split(numRegions);
        assertEquals(numRegions - 1, splits.length);

        int digits = 4;
        assertEquals(String.format("%0" + digits + "d", 3), Bytes.toString(splits[0]));
        assertEquals(String.format("%0" + digits + "d", 6), Bytes.toString(splits[1]));
        assertEquals(String.format("%0" + digits + "d", 10), Bytes.toString(splits[2]));
        assertEquals(String.format("%0" + digits + "d", 996), Bytes.toString(splits[numRegions - 2]));
    }

    @Test
    public void testDigits() {
        assertEquals(1, DecimalStringSplit.digits(0));
        assertEquals(1, DecimalStringSplit.digits(1));
        assertEquals(1, DecimalStringSplit.digits(9));
        assertEquals(2, DecimalStringSplit.digits(10));
        assertEquals(2, DecimalStringSplit.digits(99));
        assertEquals(3, DecimalStringSplit.digits(100));
        assertEquals(6, DecimalStringSplit.digits(100000));
        assertEquals(1, DecimalStringSplit.digits(-1));
    }

    @Test
    public void testSplitPoint() {
        assertEquals(1, DecimalStringSplit.splitPoint(10, 10, 0));
        assertEquals(9, DecimalStringSplit.splitPoint(10, 10, 8));
        try {
            assertEquals(1, DecimalStringSplit.splitPoint(10, 10, 9));
            fail();
        } catch (IndexOutOfBoundsException ignore) {
        }

        assertEquals(10, DecimalStringSplit.splitPoint(10, 100, 0));
        assertEquals(90, DecimalStringSplit.splitPoint(10, 100, 8));
        try {
            assertEquals(10, DecimalStringSplit.splitPoint(10, 100, 9));
            fail();
        } catch (IndexOutOfBoundsException ignore) {
        }

        assertEquals(3, DecimalStringSplit.splitPoint(30, 100, 0));
        assertEquals(6, DecimalStringSplit.splitPoint(30, 100, 1));
        assertEquals(10, DecimalStringSplit.splitPoint(30, 100, 2));
        assertEquals(13, DecimalStringSplit.splitPoint(30, 100, 3));

        assertEquals(3, DecimalStringSplit.splitPoint(333, 1000, 0));
        assertEquals(6, DecimalStringSplit.splitPoint(333, 1000, 1));
        assertEquals(9, DecimalStringSplit.splitPoint(333, 1000, 2));
        assertEquals(12, DecimalStringSplit.splitPoint(333, 1000, 3));
        assertEquals(996, DecimalStringSplit.splitPoint(333, 1000, 331));

        assertEquals(33, DecimalStringSplit.splitPoint(3, 100, 0));
        assertEquals(66, DecimalStringSplit.splitPoint(3, 100, 1));

        assertEquals(33, DecimalStringSplit.splitPoint(30, 1000, 0));
        assertEquals(66, DecimalStringSplit.splitPoint(30, 1000, 1));
        assertEquals(100, DecimalStringSplit.splitPoint(30, 1000, 2));
        assertEquals(133, DecimalStringSplit.splitPoint(30, 1000, 3));
        assertEquals(166, DecimalStringSplit.splitPoint(30, 1000, 4));
        assertEquals(200, DecimalStringSplit.splitPoint(30, 1000, 5));

        assertEquals(2, DecimalStringSplit.splitPoint(50, 100, 0));
        assertEquals(4, DecimalStringSplit.splitPoint(50, 100, 1));
    }
}
