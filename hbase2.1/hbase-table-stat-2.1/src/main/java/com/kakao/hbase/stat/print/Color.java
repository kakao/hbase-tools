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

package com.kakao.hbase.stat.print;

import org.apache.hadoop.hbase.util.Pair;

public enum Color {
    bold("1", new Pair<>("<b>", "</b>")),
    underline("4", new Pair<>("<u>", "</u>")),
    black("30", new Pair<>("<span style=\"color:black\">", "</span>")),
    red("31", new Pair<>("<span style=\"color:red\">", "</span>")),
    green("32", new Pair<>("<span style=\"color:green\">", "</span>")),
    yellow("33", new Pair<>("<span style=\"color:yellow\">", "</span>")),
    blue("34", new Pair<>("<span style=\"color:blue\">", "</span>")),
    magenta("35", new Pair<>("<span style=\"color:magenta\">", "</span>")),
    cyan("36", new Pair<>("<span style=\"color:cyan\">", "</span>")),
    white("37", new Pair<>("<span style=\"color:white\">", "</span>")),
    blackBg("40", new Pair<>("<span style=\"background:black\">", "</span>")),
    redBg("41", new Pair<>("<span style=\"background:red\">", "</span>")),
    greenBg("42", new Pair<>("<span style=\"background:green\">", "</span>"));

    public static final Color HEADER_DEFAULT = cyan;
    public static final Color HEADER_SORTED_BY_VALUE = green;
    public static final Color HEADER_SORTED_BY_DIFF = greenBg;

    static final Color CHANGED = yellow;
    static final Color LEVEL = bold;

    private static final int PAD_LIMIT = 8192;
    private final String code;
    private final Pair<String, String> htmlTag;

    Color(String code, Pair<String, String> htmlTag) {
        this.code = code;
        this.htmlTag = htmlTag;
    }

    static int lengthWithoutColor(String string, Formatter.Type formatType) {
        return clearColor(string, formatType).length();
    }

    public static String clearColor(String string, Formatter.Type formatType) {
        if (formatType == Formatter.Type.ANSI) {
            return string.replaceAll("\\033\\[.*?m", "");
        } else {
            return string.replaceAll("<.*?>", "");
        }
    }

    /**
     * Extracted from org.apache.commons.lang.StringUtils
     */
    static String leftPad(String str, int size, String padStr, Formatter.Type formatType) {
        if (str == null) {
            return null;
        }
        if (isEmpty(padStr)) {
            padStr = " ";
        }
        int padLen = padStr.length();
        int strLen = lengthWithoutColor(str, formatType);
        int pads = size - strLen;
        if (pads <= 0) {
            return str; // returns original String when possible
        }
        if (padLen == 1 && pads <= PAD_LIMIT) {
            return leftPad(str, size, padStr.charAt(0), formatType);
        }

        if (pads == padLen) {
            return padStr.concat(str);
        } else if (pads < padLen) {
            return padStr.substring(0, pads).concat(str);
        } else {
            char[] padding = new char[pads];
            char[] padChars = padStr.toCharArray();
            for (int i = 0; i < pads; i++) {
                padding[i] = padChars[i % padLen];
            }
            return new String(padding).concat(str);
        }
    }

    /**
     * Extracted from org.apache.commons.lang.StringUtils
     */
    private static String leftPad(String str, int size, char padChar, Formatter.Type formatType) {
        if (str == null) {
            return null;
        }
        int pads = size - lengthWithoutColor(str, formatType);
        if (pads <= 0) {
            return str; // returns original String when possible
        }
        if (pads > PAD_LIMIT) {
            return leftPad(str, size, String.valueOf(padChar), formatType);
        }
        return padding(pads, padChar).concat(str);
    }

    /**
     * Extracted from org.apache.commons.lang.StringUtils
     */
    private static String padding(int repeat, char padChar) throws IndexOutOfBoundsException {
        if (repeat < 0) {
            throw new IndexOutOfBoundsException("Cannot pad a negative amount: " + repeat);
        }
        final char[] buf = new char[repeat];
        for (int i = 0; i < buf.length; i++) {
            buf[i] = padChar;
        }
        return new String(buf);
    }

    /**
     * Extracted from org.apache.commons.lang.StringUtils
     */
    private static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    String build(String string, Formatter.Type formatType) {
        if (formatType == Formatter.Type.ANSI) {
            return "\033[" + code + "m" + string + "\033[0m";
        } else {
            return htmlTag.getFirst() + string + htmlTag.getSecond();
        }
    }

}
