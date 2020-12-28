package ru.moex.importer.http;

public class TradesRequester extends AbstractRequester {

    private final String tradesEndpoint = "/trades.json";

    public String requestTradesWithSess(int prevSession, int start, int limit) {
        var uri = protocol + hostname + baseEndpoint +
                tradesEndpoint +
                "?previous_session=" + prevSession +
                "&start=" + start +
                "&limit=" + limit;

        return requestData(uri);
    }

    public String requestTradesWithTradeNo(long tradeNo) {
        var uri = protocol + hostname + baseEndpoint +
                tradesEndpoint +
                "?tradeno=" + tradeNo;

        return requestData(uri);
    }
}
