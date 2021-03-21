package ru.moex.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.moex.importer.data.TradesDataElement;
import ru.moex.importer.http.TradesRequester;
import ru.moex.importer.parser.TradesJsonParser;
import ru.moex.importer.storage.Storage;
import ru.moex.importer.storage.clickhouse.TradesClickHouseStorage;

import java.time.LocalDate;

import static ru.moex.importer.Util.checkCondition;

public class TradesLoader {
    public static final int BATCH_SIZE = 5000;

    private static final Logger log = LoggerFactory.getLogger(TradesLoader.class.getName());

    TradesRequester requester = new TradesRequester();

    public static void main(String[] args) {
        var tradesLoader = new TradesLoader();
        var appConfig = AppConfig.createFromArgs(args);
        tradesLoader.processTradesData(appConfig);
    }

    private void processTradesData(AppConfig config) {
        try (var storage = new TradesClickHouseStorage(config)) {
            int totalSessionRows = 0;
            int inserted = 0;
            int skipped = 0;
            var prevSession = 3;
            LocalDate currLoadingDate = null;
            while (true) {

                // find next non-empty session
                if (inserted < 300 && skipped == 0) {
                    var found = false;
                    while (!found) {
                        if (--prevSession < 0) {
                            return;
                        }
                        currLoadingDate = getDateForSession(prevSession);
                        totalSessionRows = 0;
                        log.info("currLoadingDate = " + currLoadingDate
                                + " prev_session = " + prevSession);
                        if (!currLoadingDate.equals(LocalDate.MIN)) {
                            found = true;
                        }
                    }
                }

                log.info("Total session rows: " + totalSessionRows);

                // checking if we've already processed this data set
                if (isAlreadyUploaded(storage, prevSession, totalSessionRows+BATCH_SIZE)) {
                    skipped = BATCH_SIZE;
                    totalSessionRows += skipped;
                    log.info("skipped " + skipped + " rows");
                    continue;
                } else {
                    skipped = 0;
                }

                checkCondition(currLoadingDate.equals(LocalDate.MIN), "Illegal value for currLoadingDate");

                log.info("currLoadingDate = " + currLoadingDate
                        + " prev_session = " + prevSession
                        + " max start = " + totalSessionRows);

                var trades = requester.requestTradesWithSess(prevSession, totalSessionRows, BATCH_SIZE);
                var tradesData = new TradesJsonParser(trades).getDataElements();

                storage.batchInsertElements(tradesData);
                inserted = tradesData.size();
                log.info("Current inserted value = " + inserted);
                totalSessionRows += inserted;
                var cnt = storage.getTableRowCnt();
                log.info("Current table rows count = " + cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private LocalDate getDateForSession(int prevSession) {
        var trades = requester.requestTradesWithSess(prevSession, 0, 1);
        var tradesData = new TradesJsonParser(trades).getDataElements();
        if (tradesData.size() > 0) {
            return tradesData.get(0).getTradeDate();
        } else {
            return LocalDate.MIN;
        }
    }

    private boolean isAlreadyUploaded(Storage<TradesDataElement> storage, int prevSession, int start) {
        var trades = requester.requestTradesWithSess(prevSession, start, 1);
        var optFirstElement = new TradesJsonParser(trades).getFirstDataElement();
        var tradeNo = optFirstElement.orElse(TradesDataElement.builder().tradeNo(1L).build()).getTradeNo();

        return storage.getTableRowCntByCondition(
                TradesClickHouseStorage.TRADES_ID_COLUMN,
                String.valueOf(tradeNo)) > 0;
    }
}
