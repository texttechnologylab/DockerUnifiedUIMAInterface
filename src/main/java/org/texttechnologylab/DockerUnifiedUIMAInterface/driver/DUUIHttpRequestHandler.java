package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

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

    /**
     * A {@code record} class of the response status code and body in a byte array.
     * Provides various convenience methods for the retrieval and decoding of the body.
     *
     * @param statusCode An HTTP status code
     * @param body The response body in a byte array
     */
    public record Response(int statusCode, byte[] body) {
        /**
         * Check if the request was successful, i.e. {@code 200 <= code < 300}.
         *
         * @return {@code true} if the request was successful
         */
        public boolean ok() {
            return statusCode >= 200 && statusCode < 300;
        }

        /**
         * @return the HTTP response body as a byte array
         */
        public byte[] body() {
            return body;
        }

        /**
         * @return the HTTP response body as a UTF-8 decoded string
         */
        public String bodyUtf8() {
            return this.bodyAsString(StandardCharsets.UTF_8);
        }

        /**
         * @return the HTTP response body as a UTF-8 decoded string
         */
        public String bodyAsUtf8() {
            return this.bodyAsString(StandardCharsets.UTF_8);
        }

        /**
         * @return the HTTP response body as a UTF-8 decoded string
         */
        public String bodyAsString(Charset charset) {
            return new String(body, charset);
        }

        /**
         * @return the Base64-decoded response body as a byte array
         */
        public byte[] bodyAsBase64() {
            return Base64.getDecoder().decode(body);
        }

        /**
         * @return the response body deserialized using the {@link MessagePack#newDefaultUnpacker default msgpack unpacker }
         */
        public MessageUnpacker bodyAsMsgPack() {
            return MessagePack.newDefaultUnpacker(body);
        }

        /**
         * @return the response body wrapped in a {@link ByteArrayInputStream}
         */
        public ByteArrayInputStream bodyAsByteArrayInputStream() {
            return new ByteArrayInputStream(body);
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
    public Response getDocumentation() {
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
     * Send a GET query to the component.
     *
     * @return the {@link Response response}
     */
    public Response get() {
        HttpRequest request = this.builder.copy()
                .uri(URI.create(this.url))
                .GET()
                .build();
        return sendAsync(request);
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
     * Create a copy of this request handler with the given headers set.
     *
     * @param headers a list of name-value header pairs
     * @return the new request handler
     * @see DUUIHttpRequestHandler#headers
     */
    public DUUIHttpRequestHandler withHeaders(String... headers) {
        DUUIHttpRequestHandler clone = new DUUIHttpRequestHandler(this.client, this.url, this.builder.copy());
        clone.headers(headers);
        return clone;
    }
}
