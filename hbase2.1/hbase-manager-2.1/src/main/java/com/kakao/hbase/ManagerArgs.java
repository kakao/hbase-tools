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
import joptsimple.OptionParser;

import java.io.IOException;

public class ManagerArgs extends Args {
    public ManagerArgs(String[] args) throws IOException {
        super(args);
    }

    @Override
    protected OptionParser createOptionParser() {
        OptionParser optionParser = createCommonOptionParser();
        optionParser.accepts(OPTION_OPTIMIZE).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_BALANCE_FACTOR).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_REGION_SERVER).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_TURN_BALANCER_OFF);
        optionParser.accepts(OPTION_MOVE_ASYNC);
        optionParser.accepts(OPTION_MAX_ITERATION).withRequiredArg().ofType(Integer.class);
        optionParser.accepts(OPTION_SKIP_EXPORT);
        optionParser.accepts(OPTION_WAIT_UNTIL_FINISH);
        optionParser.accepts(OPTION_LOCALITY_THRESHOLD).withRequiredArg().ofType(Double.class);
        optionParser.accepts(OPTION_CF).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_INTERACTIVE);
        optionParser.accepts(OPTION_PHOENIX);
        return optionParser;
    }
}
