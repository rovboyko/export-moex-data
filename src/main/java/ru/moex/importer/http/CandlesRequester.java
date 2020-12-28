package ru.moex.importer.http;


import java.time.LocalDate;

import static ru.moex.importer.Util.enrichEndpoint;

public class CandlesRequester extends AbstractRequester {

    private final String candlesEndpoint = "/securities/%s/candles.json";
    private final String intervalParameter = "interval=%s";
    private final String fromParameter = "from=%s";

    public String requestCandlesForSecIdAndFrom(String secId, LocalDate from) {
        var uri = protocol + hostname + baseEndpoint +
                enrichEndpoint(candlesEndpoint, secId) +
                "?" + enrichEndpoint(intervalParameter, "10") +
                "&" + enrichEndpoint(fromParameter, from.toString());

        return requestData(uri);
    }
}
