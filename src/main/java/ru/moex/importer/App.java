package ru.moex.importer;

import ru.moex.importer.http.TradesRequester;
import ru.moex.importer.parser.TradesJsonParser;
import ru.moex.importer.storage.ClickHouseStorage;

import java.time.LocalDate;

public class App {
    public static void main(String[] args) {
        var storage = new ClickHouseStorage();
//        var maxTradeNo = storage.getTableMaxId(ClickHouseStorage.TRADES_TABLE, ClickHouseStorage.ID_COLUMN);
        var start = storage.getTableRowCntByDate(ClickHouseStorage.TRADES_TABLE,
                ClickHouseStorage.DATE_COLUMN, LocalDate.parse("2020-12-11"));

//        System.out.println("max tradeno = " + maxTradeNo);
        System.out.println("max start = " + start);

        var requester = new TradesRequester();
//        var trades = requester.requestTradesWithTradeNo(maxTradeNo + 1);
        var trades = requester.requestTradesWithStart(start + 1);
//        var trades = getTrades();
//        System.out.println("trades = " + trades.substring(0, Math.min(1000, trades.length())));
        var parser = new TradesJsonParser(trades);
        var tradesData = parser.getTradesData();
//        for (TradesDataElement el : tradesData) {
//            System.out.println(el);
//        }

        storage.batchInsertTrades(tradesData);
        System.out.println("Current rows count = " +
                storage.getTableRowCntByDate(ClickHouseStorage.TRADES_TABLE,
                        ClickHouseStorage.DATE_COLUMN, LocalDate.parse("2020-12-11")));
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
