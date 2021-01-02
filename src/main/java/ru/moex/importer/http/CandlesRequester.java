package ru.moex.importer.http;


import java.time.LocalDate;

import static ru.moex.importer.Util.enrichEndpoint;

public class CandlesRequester extends AbstractRequester {

    private final String candlesEndpoint = "/securities/%s/candles.json";
    private final String intervalParameter = "interval=%s";
    private final String fromParameter = "from=%s";
    private final String tillParameter = "till=%s";

    public String requestCandlesForSecId(String secId, LocalDate date) {
        return requestCandlesForSecId(secId, date, date);
    }

    public String requestCandlesForSecId(String secId, LocalDate fromDate, LocalDate tillDate) {
        var uri = protocol + hostname + baseEndpoint +
                enrichEndpoint(candlesEndpoint, secId) +
                "?" + enrichEndpoint(intervalParameter, "10") +
                "&" + enrichEndpoint(fromParameter, fromDate.toString()) +
                "&" + enrichEndpoint(tillParameter, tillDate.toString());

        return requestData(uri);
    }
}
