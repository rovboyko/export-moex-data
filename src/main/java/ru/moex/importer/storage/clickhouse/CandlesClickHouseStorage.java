package ru.moex.importer.storage.clickhouse;

import ru.moex.importer.AppConfig;
import ru.moex.importer.data.CandlesDataElement;
import ru.moex.importer.data.CandlesInterval;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static ru.moex.importer.AppConfig.CANDLES_INTERVAL;

public class CandlesClickHouseStorage extends AbstractClickHouseStorage<CandlesDataElement> {

    public static final String BASE_CANDLES_TABLE = "candles";
    public final String CANDLES_TABLE;

    public CandlesClickHouseStorage(AppConfig appConfig) {
        super(appConfig);
        CANDLES_TABLE = getCandlesTableName(appConfig.get(CANDLES_INTERVAL));
    }

    @Override
    PreparedStatement createPrepareStatement(Connection connection) throws SQLException {
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

    private String getCandlesTableName(String strInterval) {
        if (CandlesInterval.ONE_MINUTE.equalzz(strInterval)) {
            return BASE_CANDLES_TABLE + "_" + CandlesInterval.ONE_MINUTE.getValue();
        } else if (CandlesInterval.TEN_MINUTES.equalzz(strInterval)) {
            return BASE_CANDLES_TABLE + "_" + CandlesInterval.TEN_MINUTES.getValue();
        } else if (CandlesInterval.ONE_DAY.equalzz(strInterval)) {
            return BASE_CANDLES_TABLE + "_" + CandlesInterval.ONE_DAY.getValue();
        } else {
            return BASE_CANDLES_TABLE;
        }
    }

}
