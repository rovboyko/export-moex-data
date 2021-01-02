package ru.moex.importer;

import ru.moex.importer.data.CandlesDataElement;
import ru.moex.importer.http.CandlesRequester;
import ru.moex.importer.parser.CandlesJsonParser;
import ru.moex.importer.storage.Storage;
import ru.moex.importer.storage.clickhouse.CandlesClickHouseStorage;

import java.time.LocalDate;

import static ru.moex.importer.AppConfig.*;
import static ru.moex.importer.Util.checkAllNotNull;
import static ru.moex.importer.Util.checkNotNull;

public class CandlesLoader {

    CandlesRequester requester = new CandlesRequester();
    private final String secId;
    private final LocalDate fromDate;
    private final LocalDate tillDate;

    public CandlesLoader(AppConfig appConfig) {
        checkNotNull(appConfig, "properties must be declared");
        this.secId = checkNotNull(appConfig.get(SEC_ID), SEC_ID + " must be specified").toUpperCase();
        var fromStr = appConfig.get(FROM_DATE);
        var tillStr = appConfig.get(TILL_DATE);
        checkAllNotNull(String.format("%s and %s must be specified", fromStr, tillStr));
        this.fromDate = LocalDate.parse(fromStr);
        this.tillDate = LocalDate.parse(tillStr);
    }

    public static void main(String[] args) {
        var appConfig = AppConfig.createFromArgs(args);
        var candlesLoader = new CandlesLoader(appConfig);

        candlesLoader.processCandlesData(appConfig);
    }

    private void processCandlesData(AppConfig config) {
        try (var storage = new CandlesClickHouseStorage(config)) {
            fromDate.datesUntil(tillDate)
                    .forEach(date -> processOneDate(storage, date));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processOneDate(Storage<CandlesDataElement> storage, LocalDate date) {
        System.out.println("date = " + date + " secId = " + secId);

        var candles = requester.requestCandlesForSecId(secId, date);
//        System.out.println("candles = " + candles);

        var candlesData = new CandlesJsonParser(candles, secId).getDataElements();
        storage.batchInsertElements(candlesData);
        System.out.println(String.format("inserted %s rows", candlesData.size()));
    }
}
