package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import static java.lang.String.format;

interface ResponsiveMessageCallback {
    public void operation(String message);
}

public interface IDUUIConnectedDriver extends IDUUIDriver {
    
    public Map<String, ? extends IDUUIInstantiatedPipelineComponent> getComponents(); 

    public default Signature get_signature(String uuid) throws ResourceInitializationException, InterruptedException {
        IDUUIInstantiatedPipelineComponent comp = getComponents().get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver.");
        }

        if (comp.getSignature() != null)
            return comp.getSignature();

        Signature sig = IDUUIInstantiatedPipelineComponent.getInputOutputs(uuid, comp);
        comp.setSignature(sig);
        sig.getInputs()
            .forEach(in -> comp.getParameters().put(in.getSimpleName(), "true")); 
            
        return sig;
    }

    public default TypeSystemDescription get_typesystem(String uuid) throws ResourceInitializationException, InterruptedException {
        IDUUIInstantiatedPipelineComponent comp = getComponents().get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }
    
    public default IDUUICommunicationLayer get_communication_layer(String url, JCas jc, int timeout_ms, HttpClient client, ResponsiveMessageCallback printfunc, DUUILuaContext context, boolean skipVerification) throws Exception {
        long start = System.currentTimeMillis();
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();
        boolean fatal_error = false;
        int iError = 0;
        while(true) {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(timeout_ms))
                .GET()
                .build();

            
            long pingStart = System.nanoTime();
            boolean reachable = ping(url, client, request, 20);
            System.out.printf("[ConnectedDriver] It took %d ms to connect to container: %s \n", 
                TimeUnit.MILLISECONDS.convert(System.nanoTime() - pingStart, TimeUnit.NANOSECONDS), reachable);
            try {
                HttpResponse<byte[]> resp = null;

                boolean connectionError = true;
                int iCount = 0;
                while(connectionError && iCount<100) {

                    try {
                        resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                        connectionError = false;
                    }
                    catch (Exception e){
                        System.out.println("[ConnectedDriver] Exception occured while requesting communication layer: ");
                        System.out.println(e.getMessage()+"\t"+url);
                        if(e instanceof java.net.ConnectException){
                            Thread.sleep(timeout_ms);
                            iCount++;
                        }
                        else if(e instanceof CompletionException){
                            Thread.sleep(timeout_ms);
                            iCount++;
                        }
                    }
                }
                if (resp.statusCode()== 200) {
                    String body2 = new String(resp.body(), Charset.defaultCharset());
                    try {
                        printfunc.operation("Component lua communication layer, loading...");

                        // System.out.printf("Got script %s\n",body2);
                        IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2,"requester",context);
                        layer = lua_com;
                        printfunc.operation("Component lua communication layer, loaded.");
                        break;
                    }
                    catch(Exception e) {
                        fatal_error = true;
                        e.printStackTrace();
                        throw new Exception("Component provided a lua script which is not runnable.");
                    }
                } else if (resp.statusCode() == 404) {
                    printfunc.operation("Component provided no own communication layer implementation using fallback.");
                    break;
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if (timeElapsed > timeout_ms) {
                    throw new TimeoutException(format("The Container did not provide one succesful answer in %d milliseconds",timeout_ms));
                }

            } catch (Exception e) {

                if(fatal_error) {
                    throw e;
                }
                else{
                    Thread.sleep(2000l);
                    iError++;
                }

                if(iError>10){
                    throw e;
                }
            }
        }

        if (!skipVerification)
            verify(url, client, layer, jc);
        
        return layer;

    }

    public default boolean ping(String url, HttpClient client, HttpRequest request, int retry) throws InterruptedException {
        int tries = 0; 
        boolean success = false; 
        do {
            try {
                client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                success = true;
            } catch (Exception e) {
                TimeUnit.MILLISECONDS.sleep(500);
            }
        } while (tries++ <= retry && !success);

        return success;
    }

    public default boolean verify(String url, HttpClient client, IDUUICommunicationLayer layer, JCas jc) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            //TODO: Make this accept options to better check the instantiation!
            layer.serialize(jc, stream, new HashMap<>(0));
        }
        catch(Exception e) {
            e.printStackTrace();
            return false;
            // throw new Exception(format("The serialization step of the communication layer fails for implementing class %s", layer.getClass().getCanonicalName()));
        }
        HttpResponse<byte[]> resp = null;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                    .version(HttpClient.Version.HTTP_1_1)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(stream.toByteArray()))
                    .build();
            resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
            if (resp == null) return false;
        } catch (Exception e) {
            return false;
        }
        if (resp.statusCode() == 200) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body());
            try {
                layer.deserialize(jc, inputStream);
            }
            catch(Exception e) {
                return false;
                // System.err.printf("Caught exception deserializing response: %s\n",new String(resp.body(), StandardCharsets.UTF_8));
                // throw e;
            }
        }
        else {
            return false;
            // throw new Exception(format("The container returned response with code != 200\nResponse %s",resp.body().toString()));
        }

        return true;
    }

    public default void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException, CASException {
        IDUUIInstantiatedPipelineComponent comp = getComponents().get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }
        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        }
        else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

}
