package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.ByteReadFuture;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.utilities.helper.ArchiveUtils;
import org.texttechnologylab.utilities.helper.StringUtils;
import org.texttechnologylab.utilities.helper.TempFileHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
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

    protected Path _path;
    protected ConcurrentHashMap<DUUIPipelineComponent, AtomicInteger> _map;
    protected ConcurrentHashMap<DUUIPipelineComponent, DUUIComposer> _cMap;
    protected AtomicInteger _numDocs;
    protected AtomicInteger _processedCasCount;



    public DUUIDynamicReaderLazy(String zip_path, List<DUUIPipelineComponent> readerComponents) throws InterruptedException, CompressorException, IOException, URISyntaxException, UIMAException, SAXException {
        this(Path.of(zip_path), readerComponents);
    }

    public DUUIDynamicReaderLazy(Path zip_path, List<DUUIPipelineComponent> readerComponents) throws InterruptedException, CompressorException, IOException, URISyntaxException, UIMAException, SAXException {
        this._path = zip_path;
        this._map = new ConcurrentHashMap<>();
        this._cMap = new ConcurrentHashMap<>();
        this._processedCasCount = new AtomicInteger(0);
        //this._pipelineComponents = readerComponents;
        for (DUUIPipelineComponent readerComponent : readerComponents) {
            this._map.put(readerComponent, new AtomicInteger(0));
        }
        initAllComponents();
        initAllComposers(1);
        this._numDocs = setSize(this._map);

        // startAllComposers();

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

    public static int initComponent(DUUIPipelineComponent readerComp, Path filePath) {
        //System.out.println(readerComp.getUrl());
        String baseUrl = getSingleElement(readerComp.getUrl());
        String initPath = "/v1/init";
        String url = baseUrl + initPath;
        int nDocs = 0;

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

    class ComponentInitWithLatch implements Runnable {
        private final AtomicInteger _counter;
        private final CountDownLatch _latch;
        private DUUIPipelineComponent _component;
        private final Path _filePath;

        public ComponentInitWithLatch(DUUIPipelineComponent component, AtomicInteger counter, Path filePath, CountDownLatch latch) {
            this._counter = counter;
            this._latch = latch;
            this._component = component;
            this._filePath = filePath;
        }

        @Override
        public void run() {
            try {
                this._counter.getAndAdd(initComponent(this._component, this._filePath));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this._latch.countDown(); // Signal completion
        }
    }

    public void initAllComponents() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(this._map.size());
        int c = 0;
        Thread[] threads = new Thread[this._map.size()];
        for (Map.Entry<DUUIPipelineComponent, AtomicInteger> entry : this._map.entrySet()) {
            threads[c] = new Thread(new ComponentInitWithLatch(entry.getKey(), entry.getValue(), this._path, latch), String.valueOf(c));
            c++;
        }
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to signal completion
        latch.await(); // Waits indefinitely, or use await(long, TimeUnit)
        // Print results
        this._map.forEach((key, value) ->
                System.out.println(key.getName() + ": " + value.get()));
    }

    public void initAllComposers(int iWorker) throws IOException, URISyntaxException, UIMAException, SAXException, CompressorException {
        for (Map.Entry<DUUIPipelineComponent, AtomicInteger> entry : this._map.entrySet()) {
            DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
            DUUIComposer composer = new DUUIComposer()
                    .withSkipVerification(true)
                    .withLuaContext(ctx)
                    .withWorkers(iWorker);
            DUUIDockerDriver docker_driver = new DUUIDockerDriver();
            DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
            DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
            DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                    .withDebug(true);

            // Hinzufügen der einzelnen Driver zum Composer
            composer.addDriver(docker_driver, uima_driver
                    ,swarm_driver, remote_driver);
            composer.add(entry.getKey());
            this._cMap.put(entry.getKey(), composer);
        }
    }

    public static AtomicInteger setSize(ConcurrentHashMap<DUUIPipelineComponent, AtomicInteger> map) {
        int sum = 0;
        for (AtomicInteger value : map.values()) {
            sum += value.get();
        }
        return new AtomicInteger(sum);
    }

    public void loadCas(JCas targetCas) throws Exception {
        for (Map.Entry<DUUIPipelineComponent, AtomicInteger> entry : this._map.entrySet()) {
            if (entry.getValue().get() > 0) {
                this._cMap.get(entry.getKey()).run(targetCas);
                this._numDocs.decrementAndGet();
                entry.getValue().decrementAndGet();
                return;
            }
        }
    }
    @Override
    public AdvancedProgressMeter getProgress() {
        return null;
    }

    @Override
    public void getNextCas(JCas pCas) {
        try {
            loadCas(pCas);
            this._processedCasCount.incrementAndGet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return (this._numDocs.get() > 0);
    }

    @Override
    public long getSize() {
        return this._numDocs.get();
    }

    @Override
    public long getDone() {
        return this._processedCasCount.get();
    }
    /*
    // Class - attribute: ConcurrentQueue<Jcas> _casQueue;
    public void startAllComposers() {
        int k = 0;
        Thread[] threads = new Thread[this._map.size()];
        for (Map.Entry<DUUIPipelineComponent, AtomicInteger> entry : this._map.entrySet()) {
            threads[k] = new Thread(new ReadingThread(entry.getValue(), this._cMap.get(entry.getKey()), this._casQueue, this._numDocs), String.valueOf(k));
            k++;
        }
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
    }

    class ReadingThread implements Runnable {
        private final AtomicInteger _count;
        private final DUUIComposer _comp;
        private final ConcurrentLinkedQueue<JCas> _cQueue;
        private final AtomicInteger _nDocs;


        public ReadingThread(AtomicInteger count,
                             DUUIComposer comp, ConcurrentLinkedQueue<JCas> cQueue,
                             AtomicInteger nDocs) {
            this._count = count;
            this._comp = comp;
            this._cQueue = cQueue;
            this._nDocs = nDocs;
        }

        @Override
        public void run() {
            while (this._count.get() > 0) {
                this._count.decrementAndGet();
                JCas empt_jcas;
                try {
                    empt_jcas = JCasFactory.createJCas();
                } catch (ResourceInitializationException | CASException e) {
                    throw new RuntimeException(e);
                }
                try {
                    this._comp.run(empt_jcas);
                    this._nDocs.decrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (empt_jcas != null) {
                    this._cQueue.add(empt_jcas);
                }
            }
        }
    }
    */
}