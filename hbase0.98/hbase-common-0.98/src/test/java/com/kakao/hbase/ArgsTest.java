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

package com.kakao.hbase;

import com.kakao.hbase.common.Args;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ArgsTest {
    @Test
    public void testParseSingleLintArgsFile() throws Exception {
        String[] args = Args.parseArgsFile("testSingleLine.args", true);
        assertEquals(4, args.length);
    }

    @Test
    public void testParseMultiLintArgsFile() throws Exception {
        String[] args = Args.parseArgsFile("testMultiLine.args", true);
        assertEquals(4, args.length);
    }

    @Test
    public void testParseConf() throws Exception {
        String[] argsParam;
        Args args;
        Map<String, String> configurations;

        argsParam = new String[]{"zookeeper", "--conf=a=a1", "--conf=a=a2", "-cb=b1"};
        args = new TestBase.TestArgs(argsParam);
        configurations = args.getConfigurations();
        assertEquals(2, configurations.size());
        assertEquals("a2", configurations.get("a"));
        assertEquals("b1", configurations.get("b"));
    }
}
