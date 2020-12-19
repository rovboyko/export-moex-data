package ru.moex.importer;

import ru.moex.importer.data.TradesDataElement;
import ru.moex.importer.http.TradesRequester;
import ru.moex.importer.parser.TradesJsonParser;
import ru.moex.importer.storage.ClickHouseStorage;
import ru.moex.importer.storage.Storage;

import java.time.LocalDate;

public class App {
    public static final int BATCH_SIZE = 5000;

    TradesRequester requester = new TradesRequester();

    public static void main(String[] args) {
        var app = new App();
        var propFilename = AppConfig.getPropFileName(args).orElse(AppConfig.PROP_FILE);
        var appConfig = AppConfig.createFromFile(propFilename);
        appConfig.addPropertiesFromArgs(args);
        app.processTradesData(appConfig);
    }

    private void processTradesData(AppConfig config) {
        try (var storage = new ClickHouseStorage(config)) {
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
                        System.out.println("currLoadingDate = " + currLoadingDate
                                + " prev_session = " + prevSession);
                        if (!currLoadingDate.equals(LocalDate.MIN)) {
                            found = true;
                        }
                    }
                }

                System.out.println("Total session rows: " + totalSessionRows);

                // checking if we've already processed this data set
                if (isAlreadyUploaded(storage, prevSession, totalSessionRows+BATCH_SIZE)) {
                    skipped = BATCH_SIZE;
                    totalSessionRows += skipped;
                    System.out.println("skipped " + skipped + " rows");
                    continue;
                } else {
                    skipped = 0;
                }

                Util.checkArgument(currLoadingDate.equals(LocalDate.MIN), "Illegal value for currLoadingDate");

                System.out.println("currLoadingDate = " + currLoadingDate
                        + " prev_session = " + prevSession
                        + " max start = " + totalSessionRows);

                var trades = requester.requestTradesWithSess(prevSession, totalSessionRows, BATCH_SIZE);
                var tradesData = new TradesJsonParser(trades).getTradesData();

                storage.batchInsertTrades(tradesData);
                inserted = tradesData.size();
                System.out.println("Current inserted value = " + inserted);
                totalSessionRows += inserted;
                var cnt = storage.getTableRowCnt(ClickHouseStorage.TRADES_TABLE);
                System.out.println("Current table rows count = " + cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private LocalDate getDateForSession(int prevSession) {
        var trades = requester.requestTradesWithSess(prevSession, 0, 1);
        var tradesData = new TradesJsonParser(trades).getTradesData();
        if (tradesData.size() > 0) {
            return tradesData.get(0).getTradeDate();
        } else {
            return LocalDate.MIN;
        }
    }

    private boolean isAlreadyUploaded(Storage storage, int prevSession, int start) {
        var trades = requester.requestTradesWithSess(prevSession, start, 1);
        var optFirstElement = new TradesJsonParser(trades).getFirstTradesElement();
        var tradeNo = optFirstElement.orElse(TradesDataElement.builder().tradeNo(1L).build()).getTradeNo();

        return storage.getTableRowCntByCondition(
                ClickHouseStorage.TRADES_TABLE,
                ClickHouseStorage.ID_COLUMN,
                String.valueOf(tradeNo)) > 0;
    }
}
