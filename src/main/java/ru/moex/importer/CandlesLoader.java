package ru.moex.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.moex.importer.data.CandlesDataElement;
import ru.moex.importer.http.CandlesRequester;
import ru.moex.importer.parser.CandlesJsonParser;
import ru.moex.importer.storage.Storage;
import ru.moex.importer.storage.clickhouse.CandlesClickHouseStorage;

import java.time.LocalDate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ru.moex.importer.AppConfig.*;
import static ru.moex.importer.Util.checkAllNotNull;
import static ru.moex.importer.Util.checkNotNull;

public class CandlesLoader {

    private static final Logger log = LoggerFactory.getLogger(CandlesLoader.class.getName());

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
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final String candlesInterval = config.get(CANDLES_INTERVAL);

        try (var storage = new CandlesClickHouseStorage(config)) {
            executor.submit(() ->
                fromDate.datesUntil(tillDate)
                    .parallel()
                    .forEach(date -> processOneDate(storage, date, candlesInterval))
            ).get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private void processOneDate(Storage<CandlesDataElement> storage, LocalDate date, String candlesInterval) {
        var candles = requester.requestCandlesForSecId(secId, date, candlesInterval);
        var candlesData = new CandlesJsonParser(candles, secId).getDataElements();
        storage.batchInsertElements(candlesData);
        log.info(String.format("inserted %s rows for date = %s and secId = %s",
                candlesData.size(), date, secId));
    }
}
