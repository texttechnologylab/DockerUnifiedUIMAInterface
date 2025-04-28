package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class DUUIHttpRequestHandler {
    final HttpClient client;
    final String url;
    final HttpRequest.Builder builder;

    private DUUIHttpRequestHandler(HttpClient client, String componentUrl, HttpRequest.Builder builder) {
        this.client = client;
        this.url = componentUrl;
        this.builder = builder;
    }

    public DUUIHttpRequestHandler(HttpClient client, String componentUrl, long timeout) {
        this.client = client;
        this.url = componentUrl;
        this.builder = HttpRequest.newBuilder()
                .timeout(Duration.ofSeconds(timeout))
                .version(HttpClient.Version.HTTP_1_1);
    }

    public record Response(int statusCode, byte[] body) {
        public String bodyAsString() {
            return new String(body);
        }
    }

    Response sendAsync(HttpRequest request) {
        HttpResponse<byte[]> response = this.client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
        return new Response(response.statusCode(), response.body());
    }

    /**
     * Get the DUUI V1 documentation of the component.
     * <p/>
     * Equivalent to {@link #get(String) get("/v1/documentation")}.
     *
     * @return the {@link HttpResponse response}
     */
    public Response documentation() {
        return get("/v1/documentation");
    }

    /**
     * Send a {@link DUUIComposer#V1_COMPONENT_ENDPOINT_PROCESS DUUI V1 process} request to the component.
     *
     * @param data an array of encoded data to send to the component
     * @return the {@link Response response}
     */
    public Response process(byte[] data) {
        String endpoint = DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS;
        return post(endpoint, data);
    }

    /**
     * Send a GET query to the given endpoint on the component.
     *
     * @param path the path of the endpoint
     * @return the {@link Response response}
     */
    public Response get(String path) {
        HttpRequest request = this.builder.copy()
                .uri(URI.create(this.url + path))
                .GET()
                .build();
        return sendAsync(request);
    }

    /**
     * Send a GET query with parameters to the given endpoint on the component.
     *
     * @param path       the path of the endpoint
     * @param parameters a list of name-value pairs to be formatted as a GET query string
     * @return the {@link Response response}
     */
    public Response get(String path, String... parameters) {
        HttpRequest request = this.builder.copy()
                .uri(URI.create(this.url + path + "?" + String.join("&", parameters)))
                .GET()
                .build();
        return sendAsync(request);
    }

    /**
     * Send a POST request with the given data to the given endpoint on the component.
     *
     * @param path the path of the endpoint
     * @param data the data as a byte array
     * @return the {@link Response response}
     */
    public Response post(String path, byte[] data) {
        HttpRequest request = this.builder.copy()
                .uri(URI.create(this.url + path))
                .POST(HttpRequest.BodyPublishers.ofByteArray(data))
                .build();
        return sendAsync(request);
    }

    }
}
