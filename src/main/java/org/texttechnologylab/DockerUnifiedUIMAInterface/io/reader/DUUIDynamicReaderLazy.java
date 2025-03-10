package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.ByteReadFuture;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.utilities.helper.ArchiveUtils;
import org.texttechnologylab.utilities.helper.StringUtils;
import org.texttechnologylab.utilities.helper.TempFileHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * DUUI Dynamic Lazy file reader..
 *
 * @author Leon Hammerla
 */
public class DUUIDynamicReaderLazy implements DUUICollectionReader {

    protected String _path;
    protected List<DUUIPipelineComponent> _pipelineComponents;
    protected Vector<Integer> docsPerComponent;


    public DUUIDynamicReaderLazy(String zip_path, List<DUUIPipelineComponent> readerComponents) {
        this._path = zip_path;
        this._pipelineComponents = readerComponents;

    }

    private static String getSingleElement(List<String> lst) {
        if (lst.size() == 1) {
            return lst.getFirst();
        }
        throw new IllegalStateException("List does not contain exactly one element.");
    }

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

    public static Integer initComponent(DUUIPipelineComponent readerComp, Path filePath) throws Exception {
        //System.out.println(readerComp.getUrl());
        String baseUrl = getSingleElement(readerComp.getUrl());
        String initPath = "/v1/init";
        String url = baseUrl + initPath;
        Integer nDocs = 0;

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
                    .uri(URI.create(url))
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nDocs;
    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return null;
    }

    @Override
    public void getNextCas(JCas pCas) {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public long getDone() {
        return 0;
    }
}