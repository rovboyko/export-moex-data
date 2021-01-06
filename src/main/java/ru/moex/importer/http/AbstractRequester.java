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

public abstract class AbstractRequester {

    final String protocol = "http://";
    final String hostname = "iss.moex.com";
    final String baseEndpoint = "/iss/engines/futures/markets/forts";

    final HttpClient httpClient;

    public AbstractRequester() {
        httpClient = createHttpClient();
    }

    protected String requestData(String uri) {
        try {
            var request = HttpRequest.newBuilder()
                    .uri(new URI(uri))
                    .GET()
                    .timeout(Duration.of(10, SECONDS))
                    .build();
            return sendRequest(request);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Can't parse URI: %s", uri), e);
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
