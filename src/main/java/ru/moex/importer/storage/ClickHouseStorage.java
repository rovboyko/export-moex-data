package ru.moex.importer.storage;

import ru.moex.importer.AppConfig;
import ru.moex.importer.data.TradesDataElement;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class ClickHouseStorage implements Storage, AutoCloseable {

//    public static final String URL = "jdbc:clickhouse://127.0.0.1:9000?user=default&password=assa123";
    public static final String URL = "jdbc:clickhouse";
    public static final String TRADES_TABLE = "trades";
    public static final String DATE_COLUMN = "TRADEDATE";
    public static final String ID_COLUMN = "TRADENO";

//    private final AppConfig appConfig;
    private final String connStr;
    private final Connection connection;
    private final Statement stmt;

    public ClickHouseStorage(AppConfig appConfig) {
//        this.appConfig = appConfig;
        connStr = String.format("%s://%s:%s?user=%s&password=%s",
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
    public void batchInsertTrades(List<TradesDataElement> tradesData) {
        try (PreparedStatement pstmt = createTradesPrepareStatement()) {
            for (TradesDataElement element : tradesData) {
                addTradesElementToStatement(pstmt, element);
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Can't finish batch insert to trades table", e);
        }
    }

    @Override
    public void singleInsertTrades(TradesDataElement tradeElement) {
        try (PreparedStatement pstmt = createTradesPrepareStatement()) {
            addTradesElementToStatement(pstmt, tradeElement);
            pstmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Can't finish single insert to trades table", e);
        }
    }

    private PreparedStatement createTradesPrepareStatement() throws SQLException {
        return connection.prepareStatement("INSERT INTO " + TRADES_TABLE +
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    }

    private void addTradesElementToStatement(PreparedStatement pstmt, TradesDataElement element) throws SQLException {
        var tradeDateTime = LocalDateTime.of(element.getTradeDate(), element.getTradeTime());
        pstmt.setLong(1, element.getTradeNo());
        pstmt.setString(2, element.getBoardName());
        pstmt.setString(3, element.getSecId());
        pstmt.setDate(4, java.sql.Date.valueOf(element.getTradeDate()));
        pstmt.setTimestamp(5, java.sql.Timestamp.valueOf(tradeDateTime));
        pstmt.setBigDecimal(6, BigDecimal.valueOf(element.getPrice()));
        pstmt.setInt(7, element.getQuantity());
        pstmt.setTimestamp(8, java.sql.Timestamp.valueOf(element.getSysTime()));
        pstmt.addBatch();
    }

    @Override
    public Integer getTableRowCntByCondition(String table, String column, String expr) {
        String sql = String.format("select count(*) from %s where %s = '%s'", table, column, expr);
        return getSingleIntFromSql(sql);
    }

    @Override
    public Integer getTableRowCnt(String table) {
        String sql = String.format("select count(*) from %s", table);
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
    public Long getTableMaxId(String table, String idColumn) {
        String sql = String.format("select max(%s) from %s", idColumn, table);
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
