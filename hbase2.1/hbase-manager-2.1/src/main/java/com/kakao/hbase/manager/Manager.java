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

package com.kakao.hbase.manager;

import com.google.common.annotations.VisibleForTesting;
import com.kakao.hbase.ManagerArgs;
import com.kakao.hbase.common.Args;
import com.kakao.hbase.common.HBaseClient;
import com.kakao.hbase.common.InvalidTableException;
import com.kakao.hbase.common.util.Util;
import com.kakao.hbase.manager.command.Command;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class Manager {
    static final String INVALID_COMMAND = "Invalid command";
    private static final String INVALID_ZOOKEEPER = "Invalid zookeeper quorum";
    private static final Set<Class<? extends Command>> commandSet;

    static {
        Util.setLoggingThreshold("ERROR");

        Reflections reflections = createReflections();
        commandSet = reflections.getSubTypesOf(Command.class);
    }

    private final Connection connection;
    private final Args args;
    private final String commandName;

    public Manager(Connection connection, Args args, String commandName) {
        this.connection = connection;
        this.args = args;
        this.commandName = commandName;
    }

    public static void main(String[] args) throws Exception {
        String commandName = "";
        Args argsObject;
        try {
            if (args.length > 0)
                commandName = args[0];
            argsObject = parseArgs(args);
        } catch (IllegalArgumentException e) {
            if (commandExists(commandName)) {
                printError(INVALID_ZOOKEEPER);
                printUsage(commandName);
                System.exit(1);
            } else {
                printError(INVALID_COMMAND);
                printUsage();
                System.exit(1);
            }
            throw e;
        }

        try (Connection connection = HBaseClient.getConnection(argsObject)){
            new Manager(connection, argsObject, commandName).run();
        } catch (InvocationTargetException e) {
            printError(e.getCause().getMessage() + "\n");
            printUsage(commandName);
            System.exit(1);
        } catch (InvalidTableException e) {
            printError(e.getMessage());
            System.exit(1);
        }
    }

    private static void printError(String message) {
        System.out.println("ERROR - " + message + "\n");
    }

    private static Reflections createReflections() {
        List<ClassLoader> classLoadersList = new LinkedList<>();
        classLoadersList.add(ClasspathHelper.contextClassLoader());
        classLoadersList.add(ClasspathHelper.staticClassLoader());

        return new Reflections(new ConfigurationBuilder()
            .setScanners(new SubTypesScanner(false /* don't exclude Object.class */), new ResourcesScanner())
            .setUrls(ClasspathHelper.forManifest(ClasspathHelper.forClassLoader(
                classLoadersList.toArray(new ClassLoader[0]))))
            .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix("com.kakao.hbase.manager.command"))));
    }

    @VisibleForTesting
    static Set<Class<? extends Command>> getCommandSet() {
        return commandSet;
    }

    @VisibleForTesting
    static String getCommandUsage(String commandName) throws Exception {
        for (Class<? extends Command> c : commandSet) {
            if (c.getSimpleName().toLowerCase().equals(commandName.toLowerCase())) {
                Method usage = c.getDeclaredMethod("usage");
                return (String) usage.invoke(null);
            }
        }

        throw new IllegalArgumentException(INVALID_COMMAND);
    }

    private static boolean commandExists(String commandName) {
        for (Class<? extends Command> c : commandSet) {
            if (c.getSimpleName().toLowerCase().equals(commandName.toLowerCase())) {
                return true;
            }
        }

        return false;
    }

    private static List<String> getCommandNames() {
        List<String> commandNames = new ArrayList<>();
        for (Class<? extends Command> c : commandSet) {
            commandNames.add(c.getSimpleName().toLowerCase());
        }
        Collections.sort(commandNames);
        return commandNames;
    }

    private static void printUsage() {
        System.out.println("Usage: " + Manager.class.getSimpleName()
            + " <command> (<zookeeper quorum>|<args file>) [args...]");
        System.out.println("  commands:");
        for (String c : getCommandNames()) System.out.println("    " + c);
        System.out.println(Args.commonUsage());
    }

    @VisibleForTesting
    static Args parseArgs(String[] argsParam) throws Exception {
        if (argsParam.length == 0) throw new IllegalArgumentException(Args.INVALID_ARGUMENTS);
        if (!commandExists(argsParam[0])) throw new IllegalArgumentException(INVALID_COMMAND);

        return new ManagerArgs(Arrays.copyOfRange(argsParam, 1, argsParam.length));
    }

    private static void printUsage(String commandName) throws Exception {
        String usage = getCommandUsage(commandName);
        if (usage == null) {
            System.out.println("Usage is not implemented");
        } else {
            System.out.println(usage);
        }
    }

    public void run() throws Exception {
        try (Admin admin = connection.getAdmin()) {
            Command command = createCommand(commandName, admin, args);
            command.run();
            Util.sendAlertAfterSuccess(args, this.getClass());
        } catch (Throwable e) {
            Util.sendAlertAfterFailed(args, this.getClass(), e.getMessage());
            throw e;
        }
    }

    private Command createCommand(String commandName, Admin admin, Args args) throws Exception {
        for (Class<? extends Command> c : commandSet) {
            if (c.getSimpleName().toLowerCase().equals(commandName.toLowerCase())) {
                Constructor constructor = c.getDeclaredConstructor(Admin.class, Args.class);
                constructor.setAccessible(true);
                return (Command) constructor.newInstance(admin, args);
            }
        }

        throw new IllegalArgumentException(INVALID_COMMAND);
    }
}
