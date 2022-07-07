package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent._client;

public class DUUIRestHandler implements IDUUIConnectionHandler {
    private boolean success;
    private URI uri;

    public DUUIRestHandler() {
    }


    public void initiate(String uri) throws URISyntaxException {
        this.uri = new URI(uri + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS);
    }


    public boolean success() {
        return this.success;
    }


    public byte[] sendAwaitResponse(byte[] serializedObject) throws IOException {

        int tries = 0;
        HttpResponse<byte[]> resp = null;
        while (tries < 10) {
            tries++;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(serializedObject))
                        .version(HttpClient.Version.HTTP_1_1)
                        .build();
                resp = _client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                break;
            } catch (Exception e) {
                e.printStackTrace();
                //System.out.printf("Cannot reach endpoint trying again %d/%d...\n",tries+1,10);
            }
        }
        if (resp == null) {
            throw new IOException("Could not reach endpoint after 10 tries!");
        }

        if (resp.statusCode() == 200) {
            success = true;
            return resp.body();
        } else {
            success = false;
            return new byte[0];
        }
    }
}

