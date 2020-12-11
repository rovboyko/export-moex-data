package ru.moex.importer.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

public class TradesRequester {

    public static final String protocol = "http://";
    public static final String hostname = "iss.moex.com";
    public static final String endpoint = "/iss/engines/futures/markets/forts/trades.json";

    private final HttpClient httpClient;

    public TradesRequester() {
        httpClient = createHttpClient();
    }

    public String requestTrades(int size) {
        var uriStr = protocol + hostname + endpoint + "?size=" + size + "&limit=10";
        try {
            var request = HttpRequest.newBuilder()
                    .uri(new URI(uriStr))
                    .GET()
                    .timeout(Duration.of(1000, SECONDS))
                    .build();
            return sendRequest(request);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Can't parse URI: %s", uriStr), e);
        }
    }

    private String sendRequest(HttpRequest request) {
        try {
            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                return response.body();
            } else {
                throw new RuntimeException(String.format("Not expected http response status: %s, " +
                        "response message = %s",
                        response.statusCode(),
                        response.body()));
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Exception while performing http request", e);
        }
    }

    private static HttpClient createHttpClient() {
        return HttpClient.newBuilder()
                .build();
    }

}
