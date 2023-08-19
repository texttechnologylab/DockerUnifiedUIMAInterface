package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.documentUpdate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.CASSerializer;
import org.apache.uima.cas.impl.CasSerializerSupport;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.CasUtil;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.flow.JCasFlow_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.impl.JCasImpl;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasIOUtils;
import org.dkpro.core.io.bincas.BinaryCasWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.AnnotatorUnreachableException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIRestClient;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface IDUUIInstantiatedPipelineComponent extends IDUUIResource {

    
    public static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .executor(Runnable::run) // Forces client to use current thread.
            .connectTimeout(Duration.ofSeconds(1000)).build();

    public BlockingQueue<? extends IDUUIUrlAccessible> getInstances();

    public DUUIPipelineComponent getPipelineComponent();
    
    public void returnInstance(IDUUIUrlAccessible item) throws InterruptedException;

    public void setSignature(Signature sig);

    public Signature getSignature();

    public default boolean isWebsocket() {
        return getPipelineComponent().isWebsocket();
    }

    public default int getWebsocketElements() {
        return getPipelineComponent().getWebsocketElements();
    }

    public default Map<String,String> getParameters() {
        return getPipelineComponent().getParameters();
    }

    public default void setScale(int scale) {
        getPipelineComponent().setScale(scale);
    }
    
    public default int getScale() {
        return getPipelineComponent().getScale(1);
    }

    public default IDUUIUrlAccessible takeInstance() throws InterruptedException {
        try {
            return getInstances().take();
        } catch (InterruptedException e) {
            throw new InterruptedException(
                format("[%s][%s] Polling instances was interrupted. ", 
                    Thread.currentThread().getName(), this.getClass().getSimpleName()));
        }
    }

    public static Signature getInputOutputs(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException, InterruptedException {
        
        BiFunction<String, Map<String, List<String>>, List<Class<? extends Annotation>>> getAnnotationList = (value, json)  -> 
            json.get(value).stream()
            .map(className -> 
            {
                try {
                    
                    return (Class<? extends Annotation>)Class.forName(className);
                } catch (ClassNotFoundException e) {
                    if (!className.equals("")) { 
                        System.out.println("[ERROR] The following class was not found: " + className);;
                    }
                    return null; 
                }
            }).filter(type -> type != null)
            .collect(Collectors.toList());
        
        DUUIRestClient _handler = DUUIRestClient.getInstance(); 
        IDUUIUrlAccessible accessible = comp.takeInstance();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(accessible.generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_INPUT_OUTPUTS))
            .version(HttpClient.Version.HTTP_1_1)
            .GET()
            .build();

        HttpResponse<byte[]> resp;
        try {
             resp = _handler.send(request, 100)
                .orElseThrow(() -> 
                    new ResourceInitializationException(new Exception("Endpoint is unreachable!"))
                );
        } finally {
            comp.returnInstance(accessible);
        }
        
        if (resp.statusCode() != 200) 
            new ResourceInitializationException(
                new Exception(String.format("Expected response 200, got %d: %s", resp.statusCode(), 
                    new String(resp.body(), StandardCharsets.UTF_8))));
            
        String jsonString = new String(resp.body(), StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, List<String>> json = mapper.readValue(jsonString, Map.class);
            List<Class<? extends Annotation>> inputs = getAnnotationList.apply("inputs", json);
            List<Class<? extends Annotation>> outputs = getAnnotationList.apply("outputs", json);
            if (json.containsKey("optionals")) {
                List<Class<? extends Annotation>> optionals = getAnnotationList.apply("optionals", json);
                inputs.addAll(optionals); // TODO: Make this optional    
            }
            return new Signature(inputs, outputs);
        } catch (IOException e) {
            System.out.println("[ERROR] JSON-String of input-outputs could not be parsed!");
            return new Signature(new ArrayList<>(), new ArrayList<>());
        }
    }

    public static TypeSystemDescription getTypesystem(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException, InterruptedException {
        
        DUUIRestClient _handler = DUUIRestClient.getInstance(); 
        IDUUIUrlAccessible accessible = comp.takeInstance(); 
        //System.out.printf("Address %s\n",accessible.generateURL()+ DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(accessible.generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM))
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
                .build();

        HttpResponse<byte[]> resp;
        try {
             resp = _handler.send(request, 100)
                .orElseThrow(() -> 
                    new ResourceInitializationException(new Exception("Endpoint is unreachable!"))
                );
        } finally {
            comp.returnInstance(accessible);
        }

        
        if (resp.statusCode() != 200) {
            System.out.printf("[%s]: Endpoint did not provide typesystem, using default one...\n", uuid);
            return TypeSystemDescriptionFactory.createTypeSystemDescription();
        } 
        
        int tries = 0; 
        String body = new String(resp.body(), Charset.defaultCharset());
        String uri = null; 
        do {    
            try {
                File tmp = File.createTempFile("duui.composer", "_type");
                FileWriter writer = new FileWriter(tmp);
                writer.write(body);
                writer.flush();
                writer.close();
                uri = tmp.toURI().toString();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                try {Thread.sleep(100);} 
                catch (InterruptedException interrupted) { throw interrupted;}
            }
        } while (tries++ < 10 && uri == null);
        return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(uri);
    }

    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException, InterruptedException {
        
        DUUIRestClient _handler = DUUIRestClient.getInstance(); 
        long mutexStart = System.nanoTime();
        IDUUIUrlAccessible accessible = comp.takeInstance(); 
        long mutexEnd = System.nanoTime();
        long durationMutexWait = mutexEnd - mutexStart;
        documentUpdate(perf.getRunKey(), comp.getSignature(), "urlwait", durationMutexWait);

        try {
            // Serialization
            long serializeStart = System.nanoTime();
            IDUUICommunicationLayer layer = accessible.getCommunicationLayer();
            DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();
            String viewName = pipelineComponent.getViewName();
            JCas viewJc;
            if(viewName == null) {
                viewJc = jc;
            }
            else {
                try {
                    viewJc = jc.getView(viewName);
                }
                catch(CASException e) {
                    if(pipelineComponent.getCreateViewFromInitialView()) {
                        viewJc = jc.createView(viewName);
                        viewJc.setDocumentText(jc.getDocumentText());
                        viewJc.setDocumentLanguage(jc.getDocumentLanguage());
                    }
                    else {
                        throw e;
                    }
                }
            }

            byte[] ok;
            {
                ByteArrayOutputStream out;
                out = comp.getResourceManager().takeByteStream();
                try {
                    synchronized (viewJc) {
                        layer.serialize(viewJc,out,comp.getParameters());
    
                    }
                    ok = out.toByteArray();
                } finally {
                    comp.getResourceManager().returnByteStream(out);
                }
            }
            long serializeSize = ok.length;
            long serializeEnd = System.nanoTime();
            long durationSerialize = serializeEnd - serializeStart;
            documentUpdate(perf.getRunKey(), comp.getSignature(), "serialization", durationSerialize);

            // Annotator
            long annotatorStart = serializeEnd;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(accessible.generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                .POST(HttpRequest.BodyPublishers.ofByteArray(ok))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

            HttpResponse<byte[]> resp = _handler.send(request, 2)
                .orElseThrow(() -> 
                    new AnnotatorUnreachableException(format("[%s] %s-Could not reach endpoint after 2 tries!",
                        Thread.currentThread().getName(), perf.getRunKey())
                    )
                );

            if (resp.statusCode() != 200) {
                throw new InvalidObjectException(
                    String.format("Expected response 200, got %d: %s", resp.statusCode(), 
                    new String(resp.body(), StandardCharsets.UTF_8)));
            }

            ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
            long annotatorEnd = System.nanoTime();
            long durationAnnotator = annotatorEnd - annotatorStart;
            documentUpdate(perf.getRunKey(), comp.getSignature(), "annotator", durationAnnotator);

            // Deserialization
            long deserializeStart = annotatorEnd;
            long jcSize = 0;
            try {
                synchronized(viewJc) {
                    layer.deserialize(viewJc, st);
                    ByteArrayOutputStream siz = new ByteArrayOutputStream(viewJc.size());
                    org.apache.uima.cas.impl.Serialization.serializeCAS(viewJc.getCas(), siz);
                    jcSize = siz.size();
                }
            }
            catch(Exception e) {
                System.err.printf("Caught exception deserializing, printing response %s\n",new String(resp.body(), StandardCharsets.UTF_8));
                throw e;
            }
            long deserializeEnd = System.nanoTime();
            long durationDeserialize = deserializeEnd - deserializeStart;
            documentUpdate(perf.getRunKey(), comp.getSignature(), "deserialization", durationDeserialize);
            documentUpdate(perf.getRunKey(), comp.getSignature(), "document_size", jcSize);

            String componentKey = String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash());
            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(durationSerialize, durationDeserialize, durationAnnotator, durationMutexWait, deserializeEnd-mutexStart, componentKey, serializeSize, jc);
            documentUpdate(perf.getRunKey(), comp.getSignature(), "component_total", deserializeEnd-mutexStart);
        } finally {
            comp.returnInstance(accessible); // return url to component!
        }
    }

    public static void process_handler(JCas jc,
                                       IDUUIInstantiatedPipelineComponent comp,
                                       DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException, InterruptedException {
        
        long mutexStart = System.nanoTime();
        IDUUIUrlAccessible accessible = comp.takeInstance(); 
        long mutexEnd = System.nanoTime();

        IDUUICommunicationLayer layer = accessible.getCommunicationLayer();
        long serializeStart = System.nanoTime();

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();

        String viewName = pipelineComponent.getViewName();
        JCas viewJc;
        if(viewName == null) {
            viewJc = jc;
        }
        else {
            try {
                viewJc = jc.getView(viewName);
            }
            catch(CASException e) {
                if(pipelineComponent.getCreateViewFromInitialView()) {
                    viewJc = jc.createView(viewName);
                    viewJc.setDocumentText(jc.getDocumentText());
                    viewJc.setDocumentLanguage(jc.getDocumentLanguage());
                }
                else {
                    throw e;
                }
            }
        }
        // lua serialize call()
        layer.serialize(viewJc,out,comp.getParameters());

        // ok is the message.
        byte[] ok = out.toByteArray();
        long sizeArray = ok.length;
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;

        IDUUIConnectionHandler handler = accessible.getHandler();

        if (handler.getClass() == DUUIWebsocketAlt.class){
            JCas finalViewJc = viewJc;

            List<ByteArrayInputStream> results = handler.send(ok);

            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            ByteArrayInputStream result = null;
            try {
                
                result = layer.merge(results);
                synchronized(jc) {
                    layer.deserialize(finalViewJc, result);
                }       
            }
            catch(Exception e) {
                e.printStackTrace();
                System.err.printf("Caught exception printing response %s\n",new String(result.readAllBytes(), StandardCharsets.UTF_8));
            }

            long deserializeEnd = System.nanoTime();

            comp.returnInstance(accessible);
            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,mutexEnd-mutexStart,deserializeEnd-mutexStart, String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc);
        }
    }
}