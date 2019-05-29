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

package com.kakao.hbase.common;

import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.specific.CommandAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.Config;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.*;
import java.util.Map;
import java.util.Scanner;

public class HBaseClient {
    private static String principal = null;
    private static String password = null;
    private static Connection connection = null;

    private HBaseClient() {
    }

    private static String createJaasConfigFile(Args args) throws FileNotFoundException, UnsupportedEncodingException {
        // fixme hash collision may occur in args.hashStr()
        final String authConfFileName = "/tmp/" + "hbase-client-" + args.hashStr() + ".jaas";
        File file = new File(authConfFileName);
        if (file.exists()) return authConfFileName;

        StringBuilder sb = new StringBuilder();
        sb.append("Client {\n");
        sb.append("com.sun.security.auth.module.Krb5LoginModule required\n");
        sb.append("useTicketCache=false\n");
        if (args.has(Args.OPTION_DEBUG)) {
            sb.append("debug=true\n");
        }
        if (args.has(Args.OPTION_KEY_TAB, Args.OPTION_KEY_TAB_SHORT)) {
            sb.append("useKeyTab=true\n");
            sb.append("keyTab=\"").append(args.valueOf(Args.OPTION_KEY_TAB, Args.OPTION_KEY_TAB_SHORT)).append("\"\n");
            sb.append("principal=\"").append(principal(args)).append("\"\n");
        } else {
            sb.append("useKeyTab=false\n");
        }
        sb.append(";};");

        try (PrintWriter writer = new PrintWriter(authConfFileName, Constant.CHARSET.name())) {
            writer.print(sb);
        }
        return authConfFileName;
    }

    private static String kerberosConfigFile(Args args) {
        final String fileNameArg;
        if (args.has(Args.OPTION_KERBEROS_CONFIG)) {
            fileNameArg = (String) args.valueOf(Args.OPTION_KERBEROS_CONFIG);
        } else {
            fileNameArg = "/etc/krb5.conf";
        }
        System.out.println("Loading kerberos config from " + fileNameArg);
        return fileNameArg;
    }

    private static void loginWithPassword(final Args args, Configuration conf) throws LoginException, IOException {
        LoginContext loginContext = new LoginContext("Client", callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nameCallback = (NameCallback) callback;
                    nameCallback.setName(principal(args));
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback passwordCallback = (PasswordCallback) callback;
                    passwordCallback.setPassword(askPassword().toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        });
        loginContext.login();

        CommandAdapter.loginUserFromSubject(conf, loginContext.getSubject());
    }

    private static void updateConf(Configuration conf, String realm) {
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication", "Kerberos");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@" + realm);
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@" + realm);
    }

    private static String principal(Args args) {
        if (principal == null) {
            if (args.has(Args.OPTION_PRINCIPAL, Args.OPTION_PRINCIPAL_SHORT)) {
                principal = (String) args.valueOf(Args.OPTION_PRINCIPAL, Args.OPTION_PRINCIPAL_SHORT);
            } else {
                System.out.print("Principal: ");
                Scanner scanner = new Scanner(System.in);
                principal = scanner.nextLine();
            }
        }

        return principal;
    }

    private static String askPassword() {
        if (password == null) {
            Console console = System.console();
            if (console == null) {
                System.out.print("Password: ");
                Scanner scanner = new Scanner(System.in);
                password = scanner.nextLine();
            } else {
                password = String.valueOf(console.readPassword("Password: "));
            }
        }

        return password;
    }

    private static boolean isSecuredCluster(Args args) {
        return args.has(Args.OPTION_KERBEROS_CONFIG) || args.has(Args.OPTION_PRINCIPAL)
                || args.has(Args.OPTION_PRINCIPAL_SHORT);
    }

    private static Configuration createBaseConfiguration(Args args) {
        Configuration confDefault = new Configuration(true);
        Configuration conf = HBaseConfiguration.create(confDefault);

        conf.set("hbase.zookeeper.quorum", args.getZookeeperQuorum());
        conf.set("zookeeper.recovery.retry", "1");
        conf.set("hbase.client.retries.number", "2");
        conf.set("hbase.meta.scanner.caching", "1000");
        for (Map.Entry<String, String> config : args.getConfigurations().entrySet()) {
            conf.set(config.getKey(), config.getValue());
        }

        return conf;
    }

    private static void validateAuthentication() {
        try {
            // Is there something better?
            connection.getAdmin().getMaster();
        } catch (IOException e) {
            System.out.println("Maybe you are connecting to the secured cluster without kerberos config.\n");
        }
    }

    private static synchronized void login(Args args, Configuration conf) throws Exception {
        if (args.has(Args.OPTION_DEBUG)) {
            System.setProperty("sun.security.krb5.debug", "true");
            System.setProperty("sun.security.spnego.debug", "true");
        }

        System.setProperty("java.security.auth.login.config", createJaasConfigFile(args));
        System.setProperty("java.security.krb5.conf", kerberosConfigFile(args));

        Config krbConfig = Config.getInstance();
        final String realm;
        if (args.has(Args.OPTION_REALM)) {
            realm = (String) args.valueOf(Args.OPTION_REALM);
            System.setProperty("java.security.krb5.realm", realm);
            System.setProperty("java.security.krb5.kdc", krbConfig.getKDCList(realm));
            Config.refresh();
        } else {
            realm = krbConfig.getDefaultRealm();
        }

        updateConf(conf, realm);

        if (args.has(Args.OPTION_KEY_TAB, Args.OPTION_KEY_TAB_SHORT)) {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal(args), (String) args.valueOf(Args.OPTION_KEY_TAB, Args.OPTION_KEY_TAB_SHORT));
        } else {
            loginWithPassword(args, conf);
        }

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        System.out.println(currentUser + "\n");
    }

    public static Connection getConnection(Args args) throws Exception {
        System.out.println("Connecting to " + args.getZookeeperQuorum());

        if (args.has(Args.OPTION_DEBUG)) Util.setLoggingThreshold("WARN");

        if (connection == null) {
            Configuration conf = createBaseConfiguration(args);
            connection = ConnectionFactory.createConnection(conf);

            if (isSecuredCluster(args)) {
                login(args, conf);
            }
        }

        validateAuthentication();

        return connection;
    }
}
