package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class DUUIHttpRequestHandler {
    final HttpClient client;
    final String url;
    final HttpRequest.Builder builder;

    public DUUIHttpRequestHandler(HttpClient client, String componentUrl, long timeout) {
        this.client = client;
        this.url = componentUrl;
        this.builder = HttpRequest.newBuilder()
                .timeout(Duration.ofSeconds(timeout))
                .version(HttpClient.Version.HTTP_1_1);
    }

    private HttpRequest.Builder processBuilder() {
        return this.builder.copy()
                .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS));
    }

    private HttpRequest.Builder documentationBuilder() {
        return this.builder.copy()
                .uri(URI.create(url + "/v1/documentation"));
    }

    public ByteArrayInputStream documentation() throws Exception {
        HttpRequest request = this.documentationBuilder().GET().build();
        HttpResponse<byte[]> response = this.client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();

        if (response.statusCode() != 200) {
            String body = new String(response.body());
            throw new RuntimeException("Error %d in GET %s: %s".formatted(response.statusCode(), request.uri(), body));
        }

        return new ByteArrayInputStream(response.body());
    }

    public ByteArrayInputStream process(byte[] data) throws Exception {
        HttpRequest request = this.processBuilder().POST(HttpRequest.BodyPublishers.ofByteArray(data)).build();
        HttpResponse<byte[]> response = this.client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();

        if (response.statusCode() != 200) {
            String body = new String(response.body());
            throw new RuntimeException("Error %d in POST %s: %s".formatted(response.statusCode(), request.uri(), body));
        }

        return new ByteArrayInputStream(response.body());
    }
}
