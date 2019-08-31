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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class BalanceFactorTest {
    @Test
    public void testParseArg() throws Exception {
        String[] argsParam;
        Args args;

        argsParam = new String[]{"zookeeper", "test", "St", "--factor=NoNo"};
        args = new ManagerArgs(argsParam);
        try {
            BalanceFactor.parseArg(args);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains(BalanceFactor.MESSAGE_INVALID))
                throw e;
        }

        argsParam = new String[]{"zookeeper", "test", "St", "--factor=sS"};
        args = new ManagerArgs(argsParam);
        Assert.assertEquals(BalanceFactor.MINIMIZE_STOREFILE_SIZE_SKEW, BalanceFactor.parseArg(args));

        argsParam = new String[]{"zookeeper", "test", "St"};
        args = new ManagerArgs(argsParam);
        Assert.assertEquals(BalanceFactor.EMPTY, BalanceFactor.parseArg(args));

        argsParam = new String[]{"zookeeper", "test", "rr", "--factor=sS"};
        args = new ManagerArgs(argsParam);
        Assert.assertEquals(BalanceFactor.EMPTY, BalanceFactor.parseArg(args));
    }

    @Test
    public void testSetConf() throws Exception {
        Configuration conf = HBaseConfiguration.create(new Configuration(true));
        BalanceFactor balanceFactor;

        balanceFactor = BalanceFactor.MINIMIZE_STOREFILE_SIZE_SKEW;
        balanceFactor.setConf(conf);
        Assert.assertEquals(BalanceFactor.WEIGHT_HIGH, conf.getFloat(balanceFactor.getConfKey(), Float.MIN_VALUE), 0.0f);
        Assert.assertEquals(BalanceFactor.WEIGHT_LOW, conf.getFloat(BalanceFactor.MINIMIZE_MOVE_COUNT.getConfKey(), Float.MIN_VALUE), 0.0f);
    }
}
