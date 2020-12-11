package ru.moex.importer.storage;

import ru.moex.importer.data.TradesDataElement;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class ClickHouseStorage implements Storage, AutoCloseable {

    public static final String URL = "jdbc:clickhouse://127.0.0.1:9000";
    public static final String TRADES_TABLE = "trades";
    public static final String DATE_COLUMN = "TRADEDATE";
    public static final String ID_COLUMN = "TRADENO";

    private final Connection connection;
    private final Statement stmt;

    public ClickHouseStorage() {
        try {
            connection = DriverManager.getConnection(URL);
            stmt = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException("Can't connect to " + URL, e);
        }
    }

    @Override
    public void batchInsertTrades(List<TradesDataElement> tradesData) {
        try (PreparedStatement pstmt = createTradesPrepareStatement()) {
            for (TradesDataElement element : tradesData) {
                addTradesElementToStatement(pstmt, element);
            }
            pstmt.executeBatch();
            System.out.println("inserted " + tradesData.size() + " rows");
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
    public Integer getTableRowCntByDate(String table, String dateColumn, LocalDate date) {
        String sql = String.format("select count(*) from %s where %s = '%s'", table, dateColumn, date);
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
