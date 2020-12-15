package ru.moex.importer;

import ru.moex.importer.http.TradesRequester;
import ru.moex.importer.parser.TradesJsonParser;
import ru.moex.importer.storage.ClickHouseStorage;

import java.time.LocalDate;

public class App {
    TradesRequester requester = new TradesRequester();

    public static void main(String[] args) {
        var app = new App();
        app.processTradesData();
    }

    private void processTradesData() {
        try (var storage = new ClickHouseStorage()) {
            int totalInserted = 0;
            int inserted = 0;
            var prevSession = 3;
            LocalDate currLoadingDate = null;
            while (true) {

                // find next non-empty session
                if (inserted < 300) {
                    var found = false;
                    while (!found) {
                        currLoadingDate = getDateForSession(--prevSession);
                        totalInserted = 0;
                        System.out.println("currLoadingDate = " + currLoadingDate
                                + " prev_session = " + prevSession);
                        if (!currLoadingDate.equals(LocalDate.MIN)) {
                            found = true;
                        }
                        if (prevSession < 0) {
                            return;
                        }
                    }
                }

                Util.checkArgument(currLoadingDate.equals(LocalDate.MIN), "Illegal value for currLoadingDate");

//                var start = storage.getTableRowCntByDate(ClickHouseStorage.TRADES_TABLE,
//                        ClickHouseStorage.DATE_COLUMN, currLoadingDate);
                var start = totalInserted;
                System.out.println("currLoadingDate = " + currLoadingDate
                        + " prev_session = " + prevSession
                        + " max start = " + start);

                var trades = requester.requestTradesWithSessAndStart(prevSession, start);
                var tradesData = new TradesJsonParser(trades).getTradesData();

                storage.batchInsertTrades(tradesData);
                inserted = tradesData.size();
                System.out.println("Current inserted value = " + inserted);
                totalInserted += inserted;
                var cnt = storage.getTableRowCnt(ClickHouseStorage.TRADES_TABLE);
                System.out.println("Current rows count = " + cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private LocalDate getDateForSession(int prevSession) {
        var trades = requester.requestTradesWithSessAndLimit(prevSession, 1);
        var tradesData = new TradesJsonParser(trades).getTradesData();
        if (tradesData.size() > 0) {
            return tradesData.get(0).getTradeDate();
        } else {
            return LocalDate.MIN;
        }
    }

    private static String getTrades() {
        return "{\n" +
                "\"trades\": {\n" +
                "\t\"metadata\": {\n" +
                "\t\t\"TRADENO\": {\"type\": \"int64\"},\n" +
                "\t\t\"BOARDNAME\": {\"type\": \"string\", \"bytes\": 12, \"max_size\": 0},\n" +
                "\t\t\"SECID\": {\"type\": \"string\", \"bytes\": 36, \"max_size\": 0},\n" +
                "\t\t\"TRADEDATE\": {\"type\": \"date\", \"bytes\": 10, \"max_size\": 0},\n" +
                "\t\t\"TRADETIME\": {\"type\": \"time\", \"bytes\": 10, \"max_size\": 0},\n" +
                "\t\t\"PRICE\": {\"type\": \"double\"},\n" +
                "\t\t\"QUANTITY\": {\"type\": \"int32\"},\n" +
                "\t\t\"SYSTIME\": {\"type\": \"datetime\", \"bytes\": 19, \"max_size\": 0}\n" +
                "\t},\n" +
                "\t\"columns\": [\"TRADENO\", \"BOARDNAME\", \"SECID\", \"TRADEDATE\", \"TRADETIME\", \"PRICE\", \"QUANTITY\", \"SYSTIME\"], \n" +
                "\t\"data\": [\n" +
                "\t\t[1963314526929554626, \"RFUD\", \"EDZ0\", \"2020-12-11\", \"10:00:00\", 1.2149, 2, \"2020-12-11 10:00:01\"],\n" +
                "\t\t[1963314526929554627, \"RFUD\", \"EDZ0\", \"2020-12-11\", \"10:00:00\", 1.2149, 2, \"2020-12-11 10:00:01\"],\n" +
                "\t\t[1963314526929554628, \"RFUD\", \"EDZ0\", \"2020-12-11\", \"10:00:00\", 1.215, 5, \"2020-12-11 10:00:01\"],\n" +
                "\t\t[1892945782752044233, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73077, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044237, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73077, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044238, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73077, 3, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044239, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73078, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1952337002837837806, \"RFUD\", \"VBZ0\", \"2020-12-11\", \"10:00:24\", 3817, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044240, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73075, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044241, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73074, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044242, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73074, 3, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044243, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73074, 5, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1960781252139198107, \"RFUD\", \"BRF1\", \"2020-12-11\", \"10:00:24\", 50.51, 13, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044244, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73073, 3, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[2014824447667601412, \"RFUD\", \"MLZ0\", \"2020-12-11\", \"10:00:24\", 21549, 3, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044245, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73072, 1, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044246, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73072, 3, \"2020-12-11 10:00:24\"],\n" +
                "\t\t[1892945782752044247, \"RFUD\", \"SiZ0\", \"2020-12-11\", \"10:00:24\", 73072, 51, \"2020-12-11 10:00:24\"]\n" +
                "\t]\n" +
                "},\n" +
                "\"dataversion\": {\n" +
                "\t\"metadata\": {\n" +
                "\t\t\"data_version\": {\"type\": \"int32\"},\n" +
                "\t\t\"seqnum\": {\"type\": \"int64\"}\n" +
                "\t},\n" +
                "\t\"columns\": [\"data_version\", \"seqnum\"], \n" +
                "\t\"data\": [\n" +
                "\t\t[4835, 1607673386238]\n" +
                "\t]\n" +
                "},\n" +
                "\"trades_yields\": {\n" +
                "\t\"metadata\": {\n" +
                "\t\t\"boardid\": {\"type\": \"string\", \"bytes\": 12, \"max_size\": 0},\n" +
                "\t\t\"secid\": {\"type\": \"string\", \"bytes\": 36, \"max_size\": 0}\n" +
                "\t},\n" +
                "\t\"columns\": [\"boardid\", \"secid\"], \n" +
                "\t\"data\": [\n" +
                "\n" +
                "\t]\n" +
                "}}";
    }
}
