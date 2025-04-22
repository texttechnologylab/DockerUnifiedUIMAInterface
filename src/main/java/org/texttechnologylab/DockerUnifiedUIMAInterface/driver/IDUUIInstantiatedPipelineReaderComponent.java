package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public interface IDUUIInstantiatedPipelineReaderComponent extends IDUUIInstantiatedPipelineComponent{

    // Helper function to merge byte arrays
    private static byte[] mergeArrays(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] arr : arrays) {
            totalLength += arr.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] arr : arrays) {
            System.arraycopy(arr, 0, result, offset, arr.length);
            offset += arr.length;
        }
        return result;
    }

    public static int initComponent(IDUUIInstantiatedPipelineComponent comp, Path filePath) {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();
        comp.addComponent(queue.getValue0());
        //System.out.printf("Address %s\n",queue.getValue0().generateURL()+ DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM);
        String initPath = "/v1/init";

        int nDocs = 0;

        int tries = 0;
        while (tries < 100) {
            tries++;
            try {
                HttpClient client = HttpClient.newHttpClient();

                // Read the file bytes
                byte[] fileBytes = Files.readAllBytes(filePath);

                // Generate a unique boundary
                String boundary = "----JavaBoundary" + UUID.randomUUID();

                // Construct multipart/form-data body
                String body = "--" + boundary + "\r\n" +
                        "Content-Disposition: form-data; name=\"file\"; filename=\"" + filePath.getFileName() + "\"\r\n" +
                        "Content-Type: application/zip\r\n\r\n";

                String closingBoundary = "\r\n--" + boundary + "--\r\n";

                // Create the final request body as a byte array
                byte[] multipartBody = mergeArrays(body.getBytes(), fileBytes, closingBoundary.getBytes());

                // Build HTTP request
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(queue.getValue0().generateURL() + initPath))
                        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(multipartBody))
                        .build();

                // Send request and get response
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                //System.out.println("Response Code: " + response.statusCode());
                //System.out.println("Response Body: " + response.body());

                String jsonResponse = response.body();  // Get the response body (string)
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(jsonResponse);

                // Extract the value of for the field "nDocs"
                JsonNode nDocsNode = jsonNode.get("n_docs"); // Get the "n_docs" field

                if (nDocsNode != null) {
                    nDocs = nDocsNode.asInt(); // Convert the field to an integer
                }
                return nDocs;
            } catch (Exception e) {
                System.out.printf("Cannot reach endpoint trying again %d/%d...\n",tries+1,100);
            }
        }
        return nDocs;
    }
    
}
