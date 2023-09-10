package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Optional;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;

public class DUUIRestClient { 

    public final static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .executor(Runnable::run) // Forces client to use current thread.
            .connectTimeout(Duration.ofSeconds(1000))
            .build();

    private final static DUUIRestClient _handler = new DUUIRestClient();

    private final HttpClient _driverClient;

    public DUUIRestClient(Class<IDUUIDriver> driver, int timeout){
        _driverClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofMillis(timeout))
            .executor(Runnable::run)
            .build();
    }
    
    public DUUIRestClient(Class<IDUUIDriver> driver){
        _driverClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .executor(Runnable::run)
            .build();
    }

    private DUUIRestClient() {
        _driverClient = null;
    }

    public static DUUIRestClient getInstance() {
        return _handler;
    }

    public <T> Optional<HttpResponse<T>> send(HttpRequest request, BodyHandler<T> handler) {
        HttpClient client = _driverClient == null ? _client : _driverClient; 
        // try {
        //     HttpResponse<T> response = client.send(request, handler);
        //     return Optional.ofNullable(response);
        // } catch (IOException | InterruptedException e) {
        //     return Optional.empty();
        // }
        try {
            HttpResponse<T> response = client.sendAsync(request, handler)
                .join();
            return Optional.ofNullable(response);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<HttpResponse<byte []>> send(HttpRequest request, int repeat) {
        int tries = 0;
        Optional<HttpResponse<byte []>> response = Optional.empty();
        do {
            response = send(request, BodyHandlers.ofByteArray());
            if (response.isPresent()) 
                break;
        } while (tries++ <= repeat);

        return response; 
    }

}
