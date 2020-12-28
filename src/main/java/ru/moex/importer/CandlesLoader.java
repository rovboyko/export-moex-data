package ru.moex.importer;

import ru.moex.importer.http.CandlesRequester;
import ru.moex.importer.storage.ClickHouseStorage;
import ru.moex.importer.storage.Storage;

import java.time.LocalDate;

import static ru.moex.importer.AppConfig.SEC_ID;
import static ru.moex.importer.AppConfig.FROM_DATE;
import static ru.moex.importer.Util.checkNotNull;

public class CandlesLoader {
    public static final int BATCH_SIZE = 5000;

    CandlesRequester requester = new CandlesRequester();
    private final AppConfig appConfig;
    private final String secId;
    private final LocalDate from;

    public CandlesLoader(AppConfig appConfig) {
        this.appConfig = checkNotNull(appConfig, "properties must be declared");
        this.secId = checkNotNull(appConfig.get(SEC_ID), SEC_ID + " must be specified");
        var fromStr = checkNotNull(appConfig.get(FROM_DATE), FROM_DATE + " must be specified");
        this.from = LocalDate.parse(fromStr);
    }

    public static void main(String[] args) {
        var appConfig = AppConfig.createFromArgs(args);
        var candlesLoader = new CandlesLoader(appConfig);

        candlesLoader.processCandlesData(appConfig);
    }

    private void processCandlesData(AppConfig config) {
        try (var storage = new ClickHouseStorage(config)) {

            System.out.println("from = " + from
                    + " secId = " + secId);

            var candles = requester.requestCandlesForSecIdAndFrom(secId, from);

            System.out.println("candles = " + candles);

//            var tradesData = new TradesJsonParser(trades).getTradesData();
//
//            storage.batchInsertTrades(tradesData);
//            inserted = tradesData.size();
//            System.out.println("Current inserted value = " + inserted);
//            totalSessionRows += inserted;
//            var cnt = storage.getTableRowCnt(ClickHouseStorage.TRADES_TABLE);
//            System.out.println("Current table rows count = " + cnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
