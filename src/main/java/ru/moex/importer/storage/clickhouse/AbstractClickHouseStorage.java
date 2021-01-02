package ru.moex.importer.storage.clickhouse;

import ru.moex.importer.AppConfig;
import ru.moex.importer.data.DataElement;
import ru.moex.importer.storage.Storage;

import java.sql.*;
import java.util.List;

public abstract class AbstractClickHouseStorage<T extends DataElement> implements Storage<T>, AutoCloseable {

    public static final String URL = "jdbc:clickhouse";

    final Connection connection;
    final Statement stmt;

    public AbstractClickHouseStorage(AppConfig appConfig) {
        String connStr = String.format("%s://%s:%s?user=%s&password=%s",
                URL,
                appConfig.getDbHost(),
                appConfig.getDbPort(),
                appConfig.getDbUser(),
                appConfig.getDbPass());
        try {
            connection = DriverManager.getConnection(connStr);
            stmt = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException("Can't connect to " + connStr, e);
        }
    }

    @Override
    public void batchInsertElements(List<T> elements) {
        try (PreparedStatement pstmt = createTradesPrepareStatement()) {
            for (T element : elements) {
                addElementToStatement(pstmt, element);
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Can't finish batch insert to trades table", e);
        }
    }

    @Override
    public void insertSingleElement(T element) {
        try (PreparedStatement pstmt = createTradesPrepareStatement()) {
            addElementToStatement(pstmt, element);
            pstmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Can't finish single insert to trades table", e);
        }
    }

    abstract void addElementToStatement(PreparedStatement pstmt, T element) throws SQLException;

    abstract PreparedStatement createTradesPrepareStatement() throws SQLException;

    public abstract String getTable();

    public abstract String getIdColumn();

    @Override
    public Integer getTableRowCntByCondition(String column, String expr) {
        String sql = String.format("select count(*) from %s where %s = '%s'", getTable(), column, expr);
        return getSingleIntFromSql(sql);
    }

    @Override
    public Integer getTableRowCnt() {
        String sql = String.format("select count(*) from %s", getTable());
        return getSingleIntFromSql(sql);
    }

    private Integer getSingleIntFromSql(String sql) {
        try {
            var rs = stmt.executeQuery(sql);
            if (rs.next())
                return rs.getInt(1);
            else
                throw new RuntimeException("Empty resultSet while querying row count: " + sql);
        } catch (SQLException e) {
            throw new RuntimeException("Can't execute sql: " + sql, e);
        }
    }

    @Override
    public Long getTableMaxId() {
        String sql = String.format("select max(%s) from %s", getIdColumn(), getTable());
        try {
            var rs = stmt.executeQuery(sql);
            if (rs.next())
                return rs.getLong(1);
            else
                throw new RuntimeException("Empty resultSet while querying row count: " + sql);
        } catch (SQLException e) {
            throw new RuntimeException("Can't execute sql: " + sql, e);
        }
    }

    @Override
    public void close() throws Exception {
        stmt.close();
        connection.close();
    }
}
