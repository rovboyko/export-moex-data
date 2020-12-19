package ru.moex.importer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AppConfig {

    //General properties
    public static String PROP_FILE = "config.properties";

    //Storage properties
    public static String DB_HOST = "db.host";
    public static String DB_PORT = "db.port";
    public static String DB_USER = "db.user";
    public static String DB_PASS = "db.pass";

    //Requester properties
    public static String MOEX_PROTO = "moex.proto";
    public static String MOEX_HOST = "moex.host";
    public static String MOEX_TRADES_ENDPOINT = "moex.trades.endpoint";
    public static String MOEX_CANDLES_ENDPOINT = "moex.candles.endpoint";

    public Set<String> argProperties = Stream.of(
            "--"+DB_HOST, "--"+DB_PORT, "--"+DB_USER, "--"+DB_PASS,
            "--"+MOEX_PROTO, "--"+MOEX_HOST,
            "--"+MOEX_TRADES_ENDPOINT, "--"+MOEX_CANDLES_ENDPOINT
            )
            .collect(Collectors.toCollection(HashSet::new));

    Properties properties = new Properties();

    /**
     * Tries to find properties file in classpath and upload all props from it
     * @param filename - custom file name. Can't be null
     */
    public static AppConfig createFromFile(String filename) {
        try (InputStream input = App.class.getClassLoader().getResourceAsStream(filename)) {

            AppConfig appConfig = new AppConfig();

            if (input == null) {
                System.out.println("Sorry, unable to find " + filename);
                return appConfig;
            }

            appConfig.properties.load(input);

            return appConfig;

        } catch (IOException ex) {
            throw new RuntimeException("IOException while creating from config file");
        }
    }

    /**
     * Tries to parse args and add its to properties
     * @param args
     */
    public void addPropertiesFromArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            System.out.println("arg = " + args[i]);
            if (argProperties.contains(args[i])) {
                properties.put(args[i].replaceFirst("--", ""), args[++i]);
                System.out.println("arg = " + args[i]);
            } else {
                System.out.println("Unknown property: " + args[i]);
            }
        }
    }

    public static Optional<String> getPropFileName(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (Objects.equals(args[i], "--"+PROP_FILE)) {
                return Optional.of(args[++i]);
            }
        }
        return Optional.empty();
    }

    public String getDbHost() {
        return properties.getProperty(DB_HOST);
    }

    public String getDbPort() {
        return properties.getProperty(DB_PORT);
    }

    public String getDbUser() {
        return properties.getProperty(DB_USER);
    }

    public String getDbPass() {
        return properties.getProperty(DB_PASS);
    }

}