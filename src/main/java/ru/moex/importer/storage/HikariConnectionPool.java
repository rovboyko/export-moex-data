package ru.moex.importer.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariConnectionPool {

    private final HikariDataSource ds;

    private HikariConnectionPool(String connStr, String user, String pass) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connStr);
        config.setUsername(user);
        config.setPassword(pass);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public static HikariConnectionPool from(String url, String user, String pass) {
        return new HikariConnectionPool(url, user, pass);
    }

    public void close() {
        ds.close();
    }
}
