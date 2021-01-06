package ru.moex.importer.http;


import java.time.LocalDate;

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
                String.format(candlesEndpoint, secId) +
                "?" + String.format(intervalParameter, "10") +
                "&" + String.format(fromParameter, fromDate.toString()) +
                "&" + String.format(tillParameter, tillDate.toString());

        return requestData(uri);
    }
}
