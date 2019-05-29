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

import com.kakao.hbase.specific.RegionLoadDelegator;
import org.apache.hadoop.util.StringUtils;

public enum LoadEntry {
    Reads {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getReadRequestsCount();
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateLong(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addLong(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffLong(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsLong(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareLong(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Long.valueOf(string);
        }
    },
    Writes {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getWriteRequestsCount();
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateLong(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addLong(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffLong(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsLong(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareLong(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Long.valueOf(string);
        }
    },
    Regions {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getRegions();
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateInt(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addInteger(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffInteger(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsInteger(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareInt(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Integer.valueOf(string);
        }
    },
    Files {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getStorefiles();
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateInt(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addInteger(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffInteger(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsInteger(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareInt(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Integer.valueOf(string);
        }
    },
    FileSize {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getStorefileSizeMB();
        }

        @Override
        public String toString(Number value) {
            return value == null ? NOT_AVAILABLE : (value.toString() + "m");
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateInt(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "m/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addInteger(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffInteger(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsInteger(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareInt(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Integer.valueOf(string);
        }
    },
    FileSizeUncomp {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getStoreUncompressedSizeMB();
        }

        @Override
        public String toString(Number value) {
            return value == null ? NOT_AVAILABLE : (value.toString() + "m");
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateInt(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "m/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addInteger(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffInteger(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsInteger(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareInt(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Integer.valueOf(string);
        }
    },
    DataLocality {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return new RatioNumber(regionLoad.getStorefileSizeMB(), regionLoad.getDataLocality());
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateRatioNumber(value, duration);
            return rate == null ? NOT_AVAILABLE : (StringUtils.formatPercent(rate.doubleValue(), 2) + "/s");
        }

        @Override
        public String toString(Number value) {
            return value == null ? NOT_AVAILABLE : StringUtils.formatPercent(value.doubleValue(), 2);
        }

        @Override
        public Number add(Number one, Number two) {
            return addRatioNumber(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffRatioNumber(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsRatioNumber(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareRatioNumber(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return RatioNumber.valueOf(string);
        }
    },
    MemstoreSize {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getMemStoreSizeMB();
        }

        @Override
        public String toString(Number value) {
            return value == null ? NOT_AVAILABLE : (value.toString() + "m");
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateInt(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "m/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addInteger(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffInteger(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsInteger(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareInt(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Integer.valueOf(string);
        }
    },
    CompactedKVs {
        @Override
        public Number getValue(RegionLoadDelegator regionLoad) {
            return regionLoad.getCurrentCompactedKVs();
        }

        @Override
        public String toRateString(Number value, long duration) {
            Number rate = rateLong(value, duration);
            return rate == null ? NOT_AVAILABLE : (rate.toString() + "/s");
        }

        @Override
        public Number add(Number one, Number two) {
            return addLong(one, two);
        }

        @Override
        public Number diff(Number one, Number two) {
            return diffLong(one, two);
        }

        @Override
        public boolean equals(Number one, Number two) {
            return equalsLong(one, two);
        }

        @Override
        public int compare(Number one, Number two) {
            return compareLong(one, two);
        }

        @Override
        public Number toNumber(String string) {
            return Long.valueOf(string);
        }
    };

    public static final String NOT_AVAILABLE = "N/A";

    private static Number diffLong(Number one, Number two) {
        if (one == null) {
            if (two == null) return null;
            else return (-two.longValue());
        } else {
            if (two == null) return one.longValue();
            else return one.longValue() - two.longValue();
        }
    }

    private static Number diffInteger(Number one, Number two) {
        if (one == null) {
            if (two == null) return null;
            else return (-two.intValue());
        } else {
            if (two == null) return one.longValue();
            else return one.intValue() - two.intValue();
        }
    }

    private static Number diffRatioNumber(Number one, Number two) {
        if (one == null) {
            if (two == null) return null;
            else {
                if (two.equals(0)) {
                    return RatioNumber.ZERO.doubleValue();
                } else {
                    return -two.doubleValue();
                }
            }
        } else {
            if (two == null) return one.doubleValue();
            else {
                if (two.equals(0)) {
                    return one.doubleValue();
                } else {
                    return one.doubleValue() - two.doubleValue();
                }
            }
        }
    }

    private static Number addInteger(Number one, Number two) {
        if (one == null) {
            if (two == null) return null;
            else return two.intValue();
        } else {
            if (two == null) return one.intValue();
            else return one.intValue() + two.intValue();
        }
    }

    private static Number addRatioNumber(Number one, Number two) {
        if (one == null) {
            if (two == null) return null;
            else {
                if (two.equals(0)) {
                    return RatioNumber.ZERO;
                } else {
                    return two;
                }
            }
        } else {
            if (two == null) return one;
            else {
                if (one.equals(0)) {
                    return two;
                } else if (two.equals(0)) {
                    return one;
                } else {
                    return ((RatioNumber) one).add((RatioNumber) two);
                }
            }
        }
    }

    private static Number addLong(Number one, Number two) {
        if (one == null) {
            if (two == null) return null;
            else return two.longValue();
        } else {
            if (two == null) return one.longValue();
            else return one.longValue() + two.longValue();
        }
    }

    private static boolean equalsLong(Number one, Number two) {
        if (one == null) {
            return two == null;
        } else {
            return two != null && one.longValue() == two.longValue();
        }
    }

    private static boolean equalsInteger(Number one, Number two) {
        if (one == null) {
            return two == null;
        } else {
            return two != null && one.intValue() == two.intValue();
        }
    }

    private static boolean equalsRatioNumber(Number one, Number two) {
        if (one == null) {
            return two == null;
        } else {
            return one.equals(two);
        }
    }

    @SuppressWarnings({"BooleanMethodIsAlwaysInverted", "RedundantIfStatement"})
    private static boolean isValid(Number value, long duration) {
        if (value == null) return false;
        if (duration == 0) return false;
        return true;
    }

    private static Number rateInt(Number value, long duration) {
        if (!isValid(value, duration)) return null;

        if (value.intValue() == 0) return 0;

        double rate = value.intValue() / (duration / 1000.0);
        if (rate < 10.0) {
            return Math.round(rate * 10.0) / 10.0;
        } else {
            return Math.round(rate);
        }
    }

    private static Number rateLong(Number value, long duration) {
        if (!isValid(value, duration)) return null;

        if (value.longValue() == 0) return 0;

        double rate = value.longValue() / (duration / 1000.0);
        if (rate < 10.0) {
            return Math.round(rate * 10.0) / 10.0;
        } else {
            return Math.round(rate);
        }
    }

    private static Number rateRatioNumber(Number value, long duration) {
        if (!isValid(value, duration)) return null;

        Double number = (Double) value;
        if (number == 0.0) return 0.0;

        return number / (duration / 1000.0);
    }

    private static int compareInt(Number one, Number two) {
        int intOne = one == null ? 0 : one.intValue();
        int intTwo = two == null ? 0 : two.intValue();
        return Integer.compare(intOne, intTwo);
    }

    private static int compareRatioNumber(Number one, Number two) {
        Double valueOne = one == null ? 0 : one.doubleValue();
        Double valueTwo = two == null ? 0 : two.doubleValue();
        return valueOne.compareTo(valueTwo);
    }

    private static int compareLong(Number one, Number two) {
        long longOne = one == null ? 0 : one.longValue();
        long longTwo = two == null ? 0 : two.longValue();
        return Long.compare(longOne, longTwo);
    }

    public abstract Number getValue(RegionLoadDelegator regionLoad);

    public String toString(Number value) {
        return value == null ? NOT_AVAILABLE : value.toString();
    }

    public abstract String toRateString(Number value, long duration);

    public abstract Number add(Number one, Number two);

    public abstract Number diff(Number one, Number two);

    public abstract boolean equals(Number one, Number two);

    public abstract int compare(Number one, Number two);

    public abstract Number toNumber(String string);
}
