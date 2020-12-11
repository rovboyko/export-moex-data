package ru.moex.importer;

import ru.moex.importer.data.TradesDataElement;
import ru.moex.importer.http.TradesRequester;
import ru.moex.importer.parser.TradesJsonParser;

public class App {
    public static void main(String[] args) {
        var requester = new TradesRequester();
        var trades = requester.requestTrades(0);
        System.out.println("trades = " + trades.substring(0, 1000));
        var parser = new TradesJsonParser(trades);
        var tradesData = parser.getTradesData();
        for (TradesDataElement el : tradesData) {
            System.out.println(el);
        }
    }
}
