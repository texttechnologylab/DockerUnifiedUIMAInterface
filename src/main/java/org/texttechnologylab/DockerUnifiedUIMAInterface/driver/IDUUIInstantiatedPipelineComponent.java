package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface IDUUIInstantiatedPipelineComponent {
    public static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofSeconds(1000)).build();

    public IDUUICommunicationLayer getCommunicationLayer();
    public Triplet<IDUUIUrlAccessible,Long,Long> getComponent();
    public void addComponent(IDUUIUrlAccessible item);

    public Map<String,String> getParameters();
    public String getUniqueComponentKey();
    public void setCommunicationLayer(IDUUICommunicationLayer layer);

    public static TypeSystemDescription getTypesystem(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();
        //System.out.printf("Address %s\n",queue.getValue0().generateURL()+ DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM);

        int tries = 0;
        while(tries < 100) {
            tries++;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET()
                        .build();
                HttpResponse<byte[]> resp = _client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                if (resp.statusCode() == 200) {
                    String body = new String(resp.body(), Charset.defaultCharset());
                    File tmp = File.createTempFile("duui.composer", "_type");
                    FileWriter writer = new FileWriter(tmp);
                    writer.write(body);
                    writer.flush();
                    writer.close();
                    comp.addComponent(queue.getValue0());
                    return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.toURI().toString());
                } else {
                    comp.addComponent(queue.getValue0());
                    System.out.printf("[%s]: Endpoint did not provide typesystem, using default one...\n", uuid);
                    return TypeSystemDescriptionFactory.createTypeSystemDescription();
                }
            } catch (Exception e) {
                System.out.printf("Cannot reach endpoint trying again %d/%d...\n",tries+1,100);
            }
        }
        throw new ResourceInitializationException(new Exception("Endpoint is unreachable!"));
    }

    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = comp.getCommunicationLayer();
        long serializeStart = System.nanoTime();

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        layer.serialize(jc,out,comp.getParameters());
        // lua serialize call()

        byte[] ok = out.toByteArray();
        long sizeArray = ok.length;
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;
        int tries = 0;
        HttpResponse<byte[]> resp = null;
        while(tries < 10) {
            //System.out.printf("Address %s\n",queue.getValue0().generateURL()+ DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS);
            tries++;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(ok))
                        .version(HttpClient.Version.HTTP_1_1)
                        .build();
                resp = _client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                break;
            }
            catch(Exception e) {
                //System.out.printf("Cannot reach endpoint trying again %d/%d...\n",tries+1,10);
            }
        }
        if(resp==null) {
            throw new IOException("Could not reach endpoint after 10 tries!");
        }


        if (resp.statusCode() == 200) {
            ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            try {
                layer.deserialize(jc, st);
            }
            catch(Exception e) {
                System.err.printf("Caught exception printing response %s\n",new String(resp.body(), StandardCharsets.UTF_8));
                throw e;
            }
            long deserializeEnd = System.nanoTime();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), comp.getUniqueComponentKey(), sizeArray, jc);
            comp.addComponent(queue.getValue0());
        } else {
            comp.addComponent(queue.getValue0());
            throw new InvalidObjectException("Response code != 200, error");
        }
    }
}
