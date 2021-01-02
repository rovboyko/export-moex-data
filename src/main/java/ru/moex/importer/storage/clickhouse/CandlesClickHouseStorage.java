package ru.moex.importer.storage.clickhouse;

import ru.moex.importer.AppConfig;
import ru.moex.importer.data.CandlesDataElement;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CandlesClickHouseStorage extends AbstractClickHouseStorage<CandlesDataElement> {

    public static final String CANDLES_TABLE = "candles";

    public CandlesClickHouseStorage(AppConfig appConfig) {
        super(appConfig);
    }

    @Override
    PreparedStatement createTradesPrepareStatement() throws SQLException {
        return connection.prepareStatement("INSERT INTO " + CANDLES_TABLE +
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    @Override
    public String getTable() {
        return CANDLES_TABLE;
    }

    @Override
    public String getIdColumn() {
        throw new UnsupportedOperationException("Id column unsupported for " + CANDLES_TABLE + " table");
    }

    @Override
    void addElementToStatement(PreparedStatement pstmt, CandlesDataElement element) throws SQLException {
        pstmt.setString(1, element.getSecId());
        pstmt.setBigDecimal(2, BigDecimal.valueOf(element.getOpen()));
        pstmt.setBigDecimal(3, BigDecimal.valueOf(element.getClose()));
        pstmt.setBigDecimal(4, BigDecimal.valueOf(element.getHigh()));
        pstmt.setBigDecimal(5, BigDecimal.valueOf(element.getLow()));
        pstmt.setBigDecimal(6, BigDecimal.valueOf(element.getValue()));
        pstmt.setBigDecimal(7, BigDecimal.valueOf(element.getVolume()));
        pstmt.setTimestamp(8, java.sql.Timestamp.valueOf(element.getBegin()));
        pstmt.setTimestamp(9, java.sql.Timestamp.valueOf(element.getEnd()));
        pstmt.addBatch();
    }
}
