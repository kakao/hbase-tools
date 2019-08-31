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

import org.junit.Assert;
import org.junit.Test;

public class AlertSenderTest {
    public static final String ALERT_SCRIPT = "echo ";

    @Test
    public void testSend() {
        int sendCountBefore = AlertSender.getSendCount();
        AlertSender.send(ALERT_SCRIPT, "snapshot test");
        Assert.assertEquals(sendCountBefore + 1, AlertSender.getSendCount());
    }
}
