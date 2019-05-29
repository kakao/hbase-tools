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

import com.kakao.hbase.stat.load.LoadIO;
import jline.console.ConsoleReader;

import java.io.IOException;
import java.util.Scanner;

public class KeyInputListener implements Runnable {
    private static boolean fileLoadMode = false;
    private final TableStat tableStat;

    KeyInputListener(TableStat tableStat) {
        this.tableStat = tableStat;
    }

    public static String doAction(String input, TableStat tableStat) {
        String option = input.trim();
        if (option.length() == 0) return null;

        String result = null;
        if (option.matches("^[0-9~!@#$%^&*()_+]")) {
            result = tableStat.setSort(option);
            System.out.println(result);
            tableStat.printStat();
            return result;
        }
        switch (option) {
            case "h":
                result = TableStat.dynamicOptions();
                System.out.println(result);
                break;
            case "q":
                System.out.println("Bye!");
                tableStat.exit(0, null);
                break;
            case "d":
                result = tableStat.toggleDiffFromStart();
                if (result != null) System.out.println(result);
                tableStat.printStat();
                break;
            case "c":
                result = tableStat.toggleShowChangedOnly();
                if (result != null) System.out.println(result);
                tableStat.printStat();
                break;
            case "p":
                result = tableStat.togglePause();
                if (result != null) System.out.println(result);
                break;
            case "R":
                result = tableStat.resetDiffStartPoint();
                if (result != null) System.out.println(result);
                break;
            case "r":
                result = tableStat.toggleShowRate();
                if (result != null) System.out.println(result);
                tableStat.printStat();
                break;
            case "C":
                result = tableStat.showConnectionInfo();
                if (result != null) System.out.println(result);
                break;
            case "S":
                result = tableStat.save();
                if (result != null) System.out.println(result);
                break;
            case "L":
                result = tableStat.showFiles();
                if (!result.startsWith(LoadIO.NO_SAVED_FILE)) {
                    tableStat.pause();
                    fileLoadMode = true;
                }
                System.out.println(result);
                break;
            default:
                result = option + " is an invalid option.";
                System.out.println(result);
        }
        return result;
    }

    @Override
    public void run() {
        Scanner scanner = null;
        ConsoleReader reader = null;
        try {
            reader = new ConsoleReader();
            if (System.console() == null) throw new IOException();  // for running in intellij
        } catch (IOException e) {
            scanner = new Scanner(System.in);
        }

        //noinspection InfiniteLoopStatement
        while (true) {
            final String input;
            try {
                if (fileLoadMode) {
                    fileLoadMode = false;
                    if (scanner != null) {
                        System.out.print("File index? ");
                        input = scanner.nextLine();
                    } else {
                        input = reader.readLine("File index? ");
                    }
                    tableStat.load(input);
                    tableStat.resume();
                    continue;
                } else {
                    if (scanner != null) {
                        input = scanner.nextLine();
                    } else {
                        input = String.valueOf(Character.toChars(reader.readCharacter()));
                    }
                }
            } catch (IOException e) {
                continue;
            }

            doAction(input, tableStat);
        }
    }
}
