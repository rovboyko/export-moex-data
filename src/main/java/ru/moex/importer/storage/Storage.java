package ru.moex.importer.storage;

import ru.moex.importer.data.TradesDataElement;

import java.time.LocalDate;
import java.util.List;

public interface Storage {

    void batchInsertTrades(List<TradesDataElement> tradesData);

    void singleInsertTrades(TradesDataElement tradeElement);

    Integer getTableRowCntByDate(String table, String dateColumn, LocalDate date);

    Long getTableMaxId(String table, String idColumn);

}