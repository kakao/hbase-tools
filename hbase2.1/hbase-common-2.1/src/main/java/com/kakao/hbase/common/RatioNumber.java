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

import java.util.Objects;

public class RatioNumber extends Number implements Comparable<RatioNumber> {
    static final RatioNumber ZERO = new RatioNumber(0, 0);

    private final double ratio;
    private final double value;

    RatioNumber(double value, double ratio) {
        this.value = value;
        this.ratio = ratio;
    }

    public static RatioNumber valueOf(String valueString) {
        int pos = valueString.indexOf(":");

        if (pos > -1) {
            double value = Double.parseDouble(valueString.substring(0, pos));
            double ratio = Double.parseDouble(valueString.substring(pos + 1));
            return new RatioNumber(value, ratio);
        }

        throw new IllegalArgumentException("invalid string");
    }

    @Override
    public int intValue() {
        return (int) ratio;
    }

    @Override
    public long longValue() {
        return (long) (ratio);
    }

    @Override
    public float floatValue() {
        return (float) (ratio);
    }

    @Override
    public double doubleValue() {
        return ratio;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(RatioNumber other) {
        if (Objects.equals(this, other)) {
            return 0;
        }
        if (ratio == other.ratio && value == other.value) {
            return 0;
        }

        // otherwise see which is less
        final double first = ratio * value;
        final double second = other.ratio * other.value;
        return Double.compare(first, second);
    }

    public RatioNumber add(RatioNumber other) {
        if (other == null) {
            return new RatioNumber(this.value, this.ratio);
        } else {
            if (this.equals(RatioNumber.ZERO)) {
                return new RatioNumber(other.value, other.ratio);
            } else if (other.equals(RatioNumber.ZERO)) {
                return new RatioNumber(this.value, this.ratio);
            } else {
                double value = this.value + other.value;
                double ratio = (this.ratio * this.value + other.ratio * other.value) / value;
                return new RatioNumber(value, ratio);
            }
        }
    }

    public double value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RatioNumber that = (RatioNumber) o;

        return Double.compare(that.ratio, ratio) == 0 && Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(ratio);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return value + ":" + ratio;
    }
}
