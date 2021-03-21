package ru.moex.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class.getName());

    //General properties
    public static String PROP_FILE = "config";
    public static String DEFAULT_PROP_FILE = "config.properties";

    //Storage properties
    public static String DB_HOST = "db.host";
    public static String DB_PORT = "db.port";
    public static String DB_USER = "db.user";
    public static String DB_PASS = "db.pass";

    //Requester properties
    public static String MOEX_PROTO = "moex.proto";
    public static String MOEX_HOST = "moex.host";
    public static String MOEX_FUTURES_ENDPOINT = "futures.endpoint";
    public static String MOEX_TRADES_ENDPOINT = "moex.trades.endpoint";
    public static String MOEX_CANDLES_ENDPOINT = "moex.candles.endpoint";

    //Data properties
    public static String SEC_ID = "sec.id";
    public static String FROM_DATE = "from.date";
    public static String TILL_DATE = "till.date";
    public static String CANDLES_INTERVAL = "candles.interval";

    public Set<String> argProperties = Stream.of("--"+PROP_FILE,
            "--"+DB_HOST, "--"+DB_PORT, "--"+DB_USER, "--"+DB_PASS,
            "--"+MOEX_PROTO, "--"+MOEX_HOST,
            "--"+MOEX_TRADES_ENDPOINT, "--"+MOEX_CANDLES_ENDPOINT,
            "--"+SEC_ID, "--"+FROM_DATE, "--"+TILL_DATE,
            "--"+CANDLES_INTERVAL
            )
            .collect(Collectors.toCollection(HashSet::new));

    private final Properties properties = new Properties();

    public static AppConfig createFromArgs(String[] args) {
        var propFilename = getPropFileName(args).orElse(AppConfig.DEFAULT_PROP_FILE);
        var appConfig = createFromFile(propFilename);
        appConfig.addPropertiesFromArgs(args);
        return appConfig;
    }

    /**
     * Tries to find properties file in classpath and upload all props from it
     * @param filename - custom file name. Can't be null
     */
    public static AppConfig createFromFile(String filename) {
        try (InputStream input = TradesLoader.class.getClassLoader().getResourceAsStream(filename)) {

            AppConfig appConfig = new AppConfig();

            if (input != null) {
                appConfig.properties.load(input);
            } else {
                log.debug("Unable to find " + filename + " trying to load it from filesystem");
                try (InputStream fileInput = new FileInputStream(filename)) {
                    appConfig.properties.load(fileInput);
                }
            }
            log.info("Config data was successfully uploaded from " + filename);
            return appConfig;

        } catch (IOException ex) {
            log.error("IOException while trying to load config from file " + filename);
            return new AppConfig();
        }
    }

    /**
     * Tries to parse args and add its to properties
     * @param args - args to be added
     */
    public void addPropertiesFromArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            log.info("arg = " + args[i]);
            if (argProperties.contains(args[i])) {
                properties.put(args[i].replaceFirst("--", ""), args[++i]);
                log.info("arg = " + args[i]);
            } else {
                log.warn("Unknown property: " + args[i]);
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

    public String get(String propName) {
        return properties.getProperty(propName);
    }

}
