package ru.moex.importer.storage.clickhouse;

import ru.moex.importer.AppConfig;
import ru.moex.importer.data.TradesDataElement;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class TradesClickHouseStorage extends AbstractClickHouseStorage<TradesDataElement> {

    public static final String TRADES_TABLE = "trades";
    public static final String TRADES_ID_COLUMN = "TRADENO";

    public TradesClickHouseStorage(AppConfig appConfig) {
        super(appConfig);
    }

    @Override
    PreparedStatement createTradesPrepareStatement() throws SQLException {
        return connection.prepareStatement("INSERT INTO " + TRADES_TABLE +
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    }

    @Override
    public String getTable() {
        return TRADES_TABLE;
    }

    @Override
    public String getIdColumn() {
        return TRADES_ID_COLUMN;
    }

    @Override
    void addElementToStatement(PreparedStatement pstmt, TradesDataElement element) throws SQLException {
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
}
