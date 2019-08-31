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

package com.kakao.hbase.stat;

import com.kakao.hbase.common.Args;
import joptsimple.OptionParser;

import java.io.IOException;

public class StatArgs extends Args {
    public StatArgs(String[] args) throws IOException {
        super(args);
    }

    protected OptionParser createOptionParser() {
        OptionParser optionParser = createCommonOptionParser();
        optionParser.accepts(OPTION_REGION);
        optionParser.accepts(OPTION_INTERVAL).withRequiredArg().ofType(Integer.class);
        optionParser.accepts(OPTION_OUTPUT).withRequiredArg().ofType(String.class);
        optionParser.accepts(OPTION_REGION_SERVER).withOptionalArg().ofType(String.class);
        optionParser.accepts(OPTION_HTTP_PORT).withRequiredArg().ofType(Integer.class);
        return optionParser;
    }
}