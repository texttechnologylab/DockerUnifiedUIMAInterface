package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;

public class DUUIRestClient { 

    public final static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .executor(Runnable::run) // Forces client to use current thread.
            .connectTimeout(Duration.ofSeconds(1000)).build();

    private final static DUUIRestClient _handler = new DUUIRestClient();

    private final HttpClient _driverClient;

    public DUUIRestClient(Class<IDUUIDriver> driver, int timeout){
        _driverClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(timeout))
            .executor(Runnable::run)
            .build();
    }
    
    public DUUIRestClient(Class<IDUUIDriver> driver){
        _driverClient = HttpClient.newBuilder()
            .executor(Runnable::run)
            .build();
    }

    private DUUIRestClient() {
        _driverClient = null;
    }

    public static DUUIRestClient getInstance() {
        return _handler;
    }

    public Optional<HttpResponse<byte []>> send(HttpRequest request) {
        HttpClient client = _driverClient == null ? _client : _driverClient; 
        try {
            HttpResponse<byte []> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            return Optional.ofNullable(response);
        } catch (IOException | InterruptedException e) {
            return Optional.empty();
        }
    }

    public Optional<HttpResponse<byte []>> send(HttpRequest request, int repeat) {
        int tries = 0;
        Optional<HttpResponse<byte []>> response = Optional.empty();
        do {
            response = send(request);
            if (response.isPresent()) 
                break;
        } while (tries++ <= repeat);

        return response; 
    }

}
