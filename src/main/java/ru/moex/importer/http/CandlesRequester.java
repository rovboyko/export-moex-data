package ru.moex.importer.http;


import ru.moex.importer.data.CandlesInterval;

import java.time.LocalDate;

public class CandlesRequester extends AbstractRequester {

    private final String candlesEndpoint = "/securities/%s/candles.json";
    private final String intervalParameter = "interval=%s";
    private final String fromParameter = "from=%s";
    private final String tillParameter = "till=%s";

    public String requestCandlesForSecId(String secId, LocalDate date, String interval) {
        return requestCandlesForSecId(secId, date, date, interval);
    }

    public String requestCandlesForSecId(String secId, LocalDate fromDate, LocalDate tillDate, String strInterval) {
        var uri = protocol + hostname + baseEndpoint +
                String.format(candlesEndpoint, secId) +
                "?" + String.format(intervalParameter, getCandlesInterval(strInterval)) +
                "&" + String.format(fromParameter, fromDate.toString()) +
                "&" + String.format(tillParameter, tillDate.toString());

        return requestData(uri);
    }

    private String getCandlesInterval(String strInterval) {
        if (CandlesInterval.ONE_MINUTE.equalzz(strInterval)) {
            return CandlesInterval.ONE_MINUTE.getValueForRequest();
        } else if (CandlesInterval.TEN_MINUTES.equalzz(strInterval)) {
            return CandlesInterval.TEN_MINUTES.getValueForRequest();
        } else if (CandlesInterval.ONE_DAY.equalzz(strInterval)) {
            return CandlesInterval.ONE_DAY.getValueForRequest();
        } else {
            return "10";
        }
    }
}
