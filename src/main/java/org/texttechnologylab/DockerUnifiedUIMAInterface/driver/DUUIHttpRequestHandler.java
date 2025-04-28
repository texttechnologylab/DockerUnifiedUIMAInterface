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

    /**
     * Adds the given name-value pair to the set of headers for this request
     *
     * @param name  the header name
     * @param value the header value
     * @see HttpRequest.Builder#header
     */
    public void header(String name, String value) {
        this.builder.header(name, value);
    }

    /**
     * Adds the given name-value pairs to the set of headers for this request.
     *
     * @param headers the list of name-value pairs
     * @see HttpRequest.Builder#headers
     */
    public void headers(String... headers) {
        this.builder.headers(headers);
    }

    /**
     * Set the given header to the given value. Overwrites previously set values.
     *
     * @param name  the name of the header to set
     * @param value the value of the header
     * @see HttpRequest.Builder#setHeader
     */
    public void setHeader(String name, String value) {
        this.builder.setHeader(name, value);
    }

    /**
     * Adds the given name-value pairs to the set of headers for this request.
     * <p/>
     * Equivalent to {@link #setHeader(String, String) headers.forEach(handler::setHeader)}.
     *
     * @param headers a {@link Map} of name-value pairs
     * @see HttpRequest.Builder#headers
     */
    public void setHeaders(Map<String, String> headers) {
        headers.forEach(this::setHeader);
    }

    /**
     * Create a copy of this request handler with the given headers set.
     *
     * @param headers a list of name-value header pairs.
     * @return the new request handler.
     * @see DUUIHttpRequestHandler#headers
     */
    public DUUIHttpRequestHandler withHeaders(String... headers) {
        DUUIHttpRequestHandler clone = new DUUIHttpRequestHandler(this.client, this.url, this.builder.copy());
        clone.headers(headers);
        return clone;
    }

    /**
     * Create a copy of this request handler with the given headers set.
     *
     * @param headers a map of name-value header pairs.
     * @return the new request handler.
     * @see DUUIHttpRequestHandler#headers
     */
    public DUUIHttpRequestHandler withHeaders(Map<String, String> headers) {
        DUUIHttpRequestHandler clone = new DUUIHttpRequestHandler(this.client, this.url, this.builder.copy());
        headers.forEach(clone::setHeader);
        return clone;
    }
}
