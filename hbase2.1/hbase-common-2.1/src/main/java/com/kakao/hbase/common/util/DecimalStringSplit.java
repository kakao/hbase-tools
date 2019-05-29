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

import com.google.common.annotations.VisibleForTesting;

public class DecimalStringSplit implements org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm {
    private static final int MAX_NUM_REGIONS = 10000;
    private final int cardinality;

    public DecimalStringSplit(int cardinality) {
        this.cardinality = cardinality;
    }

    @VisibleForTesting
    static int digits(int number) {
        return number == 0 ? 1 : (int) (Math.log10(Math.abs(number)) + 1);
    }

    @VisibleForTesting
    static int splitPoint(int numRegions, int cardinality, int index) {
        if (numRegions < index + 2) throw new IndexOutOfBoundsException("numRegions < index + 2");
        if (numRegions > cardinality) throw new IllegalArgumentException("numRegions > cardinality");

        return (int) (Math.floor((double) cardinality / (double) numRegions * (double)(index + 1)));
    }

    @Override
    public byte[] split(byte[] start, byte[] end) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public byte[][] split(int numRegions) {
        if (numRegions > MAX_NUM_REGIONS)
            throw new IllegalArgumentException("You can not split table into more than "
                    + MAX_NUM_REGIONS + " regions.");

        byte[][] splits = new byte[numRegions - 1][];

        int digits = digits(cardinality);
        for (int i = 0; i < numRegions - 1; i++) {
            splits[i] = String.format("%0" + digits + "d", splitPoint(numRegions, cardinality, i)).getBytes();
        }

        return splits;
    }

    @Override
    public byte[][] split(byte[] start, byte[] end, int numSplits, boolean inclusive) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public byte[] firstRow() {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public byte[] lastRow() {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public void setFirstRow(String userInput) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public void setLastRow(String userInput) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public byte[] strToRow(String input) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public String rowToStr(byte[] row) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public String separator() {
        throw new IllegalStateException("Not implemented yet");
    }

    public void setFirstRow(byte[] userInput) {
        throw new IllegalStateException("Not implemented yet");
    }

    public void setLastRow(byte[] userInput) {
        throw new IllegalStateException("Not implemented yet");
    }
}
