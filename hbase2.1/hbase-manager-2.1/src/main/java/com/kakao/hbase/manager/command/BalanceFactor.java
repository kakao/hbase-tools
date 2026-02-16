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

package com.kakao.hbase.manager.command;

import com.kakao.hbase.common.Args;
import com.kakao.hbase.ManagerArgs;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public enum BalanceFactor {
    MINIMIZE_MOVE_COUNT {
        @Override
        public String shortName() {
            return "MV";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.moveCost";
        }
    }, MINIMIZE_REGION_COUNT_SKEW {
        @Override
        public String shortName() {
            return "RC";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.regionCountCost";
        }
    }, MINIMIZE_TABLE_SKEW {
        @Override
        public String shortName() {
            return "TS";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.tableSkewCost";
        }
    }, MINIMIZE_READ_REQUEST_SKEW {
        @Override
        public String shortName() {
            return "RR";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.readRequestCost";
        }
    }, MINIMIZE_WRITE_REQUEST_SKEW {
        @Override
        public String shortName() {
            return "WR";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.writeRequestCost";
        }
    }, MINIMIZE_MEMSTORE_SIZE_SKEW {
        @Override
        public String shortName() {
            return "MS";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.memstoreSizeCost";
        }
    }, MINIMIZE_STOREFILE_SIZE_SKEW {
        @Override
        public String shortName() {
            return "SS";
        }

        @Override
        String getConfKey() {
            return "hbase.master.balancer.stochastic.storefileSizeCost";
        }
    }, EMPTY {
        @Override
        public String shortName() {
            return "EM";
        }

        @Override
        String getConfKey() {
            return null;
        }
    };

    static final String MESSAGE_INVALID = "Invalid balance factor";
    static final float WEIGHT_HIGH = 1000000.f;
    static final float WEIGHT_LOW = 0.0001f;
    private static final char INDENT_CHAR = ' ';

    public static StringBuilder usage(int indent) {
        StringBuilder sb = new StringBuilder();
        for (BalanceFactor balanceFactor : BalanceFactor.values()) {
            if (balanceFactor != EMPTY)
                sb.append(StringUtils.repeat(INDENT_CHAR, indent)).append(balanceFactor.shortName()).append(" : ").append(balanceFactor.name()).append("\n");
        }
        return sb;
    }

    public static BalanceFactor parseArg(Args args) {
        if (((String) args.getOptionSet().nonOptionArguments().get(2)).toUpperCase().equals(BalanceRule.ST.name())) {
            if (args.getOptionSet().has(ManagerArgs.OPTION_BALANCE_FACTOR)) {
                String factor = ((String) args.getOptionSet().valueOf(ManagerArgs.OPTION_BALANCE_FACTOR)).toUpperCase();
                try {
                    return BalanceFactor.valueOf(factor);
                } catch (IllegalArgumentException e) {
                    for (BalanceFactor balanceFactor : BalanceFactor.values()) {
                        if (balanceFactor.shortName().equals(factor)) return balanceFactor;
                    }
                    throw new IllegalArgumentException(MESSAGE_INVALID);
                }
            }
        }
        return EMPTY;
    }

    public static void printFactor(BalanceFactor balanceFactor) {
        if (balanceFactor != EMPTY) {
            System.out.println("Balanced by " + balanceFactor.name());
        }
    }

    protected abstract String shortName();

    public void setConf(Configuration conf) {
        for (BalanceFactor balanceFactor : BalanceFactor.values()) {
            if (balanceFactor != EMPTY) {
                conf.setFloat(balanceFactor.getConfKey(), balanceFactor == this ? WEIGHT_HIGH : WEIGHT_LOW);
            }
        }
    }

    abstract String getConfKey();
}
