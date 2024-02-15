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

package com.kakao.hbase.stat.webapp;

import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.Constant;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.stat.KeyInputListener;
import com.kakao.hbase.stat.TableStat;
import com.kakao.hbase.stat.print.Formatter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.*;
import java.util.Enumeration;

public class WebApp {
    private static WebApp instance = null;
    private final HttpServer server;
    private boolean isServerRunning = false;

    private WebApp(Args args, TableStat tableStat) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port(args)), 0);
        server.createContext("/", new RootHandler(args));
        server.createContext("/stat", new StatHandler(tableStat.getFormatter()));
        server.createContext("/jquery", new JQueryHandler());
        server.createContext("/keyInput", new KeyInputHandler(tableStat));
        server.setExecutor(null);
    }

    public static WebApp getInstance(Args args, TableStat tableStat) throws IOException {
        if (instance == null) {
            instance = new WebApp(args, tableStat);
        }
        return instance;
    }

    private static String printInetAddresses(int port) throws SocketException {
        StringBuilder sb = new StringBuilder();
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (!(inetAddress instanceof Inet4Address))
                    continue;
                if (inetAddress.toString().contains("127.0.0.1"))
                    continue;
                sb.append("Starting HTTP server - http:/").append(inetAddress.toString()).append(":").append(port).append("\n");
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    private static int port(Args args) {
        final int port;
        if (args.has(Args.OPTION_HTTP_PORT)) {
            port = (Integer) args.valueOf(Args.OPTION_HTTP_PORT);
        } else {
            port = 0;
        }
        return port;
    }

    public void startHttpServer() {
        if (isServerRunning)
            return;

        server.start();
        isServerRunning = true;
        System.out.println(printInetAddresses());
    }

    public String printInetAddresses() {
        try {
            return printInetAddresses(server.getAddress().getPort());
        } catch (SocketException e) {
            return "N/A";
        }
    }

    private static class RootHandler implements HttpHandler {
        private static String html = null;
        private final Args args;

        RootHandler(Args args) {
            this.args = args;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            if (html == null) {
                String indexString = Util.getResource("webapp/index.html");
                html = indexString.replace("1000 * 10 /*interval*/", String.valueOf(args.getIntervalMS()));
            }
            t.sendResponseHeaders(200, html.getBytes(Constant.CHARSET).length);
            OutputStream os = t.getResponseBody();
            os.write(html.getBytes(Constant.CHARSET));
            os.close();
        }
    }

    private static class JQueryHandler implements HttpHandler {
        private static String html = null;

        @Override
        public void handle(HttpExchange t) throws IOException {
            if (html == null) {
                html = Util.getResource("webapp/js/jquery-2.1.4.min.js");
            }
            t.sendResponseHeaders(200, html.getBytes(Constant.CHARSET).length);
            OutputStream os = t.getResponseBody();
            os.write(html.getBytes(Constant.CHARSET));
            os.close();
        }
    }

    private static class StatHandler implements HttpHandler {
        private final Formatter formatter;

        StatHandler(Formatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            String statString = formatter.toHtmlString();

            t.sendResponseHeaders(200, statString.getBytes(Constant.CHARSET).length);
            OutputStream os = t.getResponseBody();
            os.write(statString.getBytes(Constant.CHARSET));
            os.close();
        }
    }

    private static class KeyInputHandler implements HttpHandler {
        private final TableStat tableStat;

        KeyInputHandler(TableStat tableStat) {
            this.tableStat = tableStat;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            InputStream inputStream = t.getRequestBody();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String keyInput = URLDecoder.decode(reader.readLine().replace("key=", ""), Constant.CHARSET.displayName());

            String result;
            if (keyInput.equals("q") || keyInput.equals("S") || keyInput.equals("L")) {
                result = "";
            } else {
                result = KeyInputListener.doAction(keyInput, tableStat);
                if (result == null) {
                    result = "";
                }
            }
            t.sendResponseHeaders(200, result.getBytes(Constant.CHARSET).length);
            OutputStream os = t.getResponseBody();
            os.write(result.getBytes(Constant.CHARSET));
            os.close();
        }
    }
}
