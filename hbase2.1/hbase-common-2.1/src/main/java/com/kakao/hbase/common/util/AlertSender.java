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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

public class AlertSender {
    private static AtomicInteger sendCounter = new AtomicInteger();

    private AlertSender() {
    }

    static void send(String alertScript, String message) {
        try {
            Process alert = Runtime.getRuntime().exec(alertScript + " \"" + message + "\"");
            alert.waitFor();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(alert.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }

                int exitValue = alert.exitValue();
                if (exitValue != 0) {
                    System.out.println("Exit Code:" + exitValue);
                    System.out.println("Message: " + sb.toString());
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        sendCounter.incrementAndGet();
    }

    @VisibleForTesting
    public static int getSendCount() {
        return sendCounter.get();
    }
}
