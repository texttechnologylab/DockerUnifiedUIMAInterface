package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.AnnotatorSignature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface IDUUIInstantiatedPipelineComponent {

    public static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofSeconds(1000)).build();

    public static final Map<AnnotatorSignature, Object> currentSignatures = new ConcurrentHashMap<>(); 

    public String getUniqueComponentKey();

    public ConcurrentLinkedQueue<? extends IDUUIUrlAccessible> getInstances();

    public DUUIPipelineComponent getPipelineComponent();
    
    public void addComponent(IDUUIUrlAccessible item);


    public default boolean isWebsocket() {
        return getPipelineComponent().isWebsocket();
    }

    public default int getWebsocketElements() {
        return getPipelineComponent().getWebsocketElements();
    }

    public default Map<String,String> getParameters() {
        return getPipelineComponent().getParameters();
    }
    
    public default int getScale() {
        return getPipelineComponent().getScale(1);
    }

    public default Triplet<IDUUIUrlAccessible,Long,Long> getComponent() {
    
        long mutexStart = System.nanoTime();
        IDUUIUrlAccessible inst = getInstances().poll();

        while(inst == null) {
            inst = getInstances().poll();
        }

        long mutexEnd = System.nanoTime();
        
        return Triplet.with(inst,mutexStart,mutexEnd);
    }

    public static AnnotatorSignature getInputOutputs(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException {

        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();
        int tries = 0;
        while(tries < 100) {
            tries++;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_INPUT_OUTPUTS))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET()
                        .build();
                        
                comp.addComponent(queue.getValue0());

                HttpResponse<byte[]> resp = _client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                if (resp.statusCode() == 200) {

                    String jsonString = new String(resp.body(), StandardCharsets.UTF_8);
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, List<String>> json = null;
                    try {
                        json = mapper.readValue(jsonString, Map.class);
                    } catch (IOException e) {
                    }

                    if (json == null) throw new Exception("JSON-String of input-outputs could not be parsed!");

                    final Map<String, List<String>> jsonFinal = json;
                    Function<String, List<Class<? extends Annotation>>> getAnnotationList = value -> jsonFinal.get(value).stream()
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
                    
                    List<Class<? extends Annotation>> inputs = getAnnotationList.apply("inputs");

                    List<Class<? extends Annotation>> outputs = getAnnotationList.apply("outputs");

                    return new AnnotatorSignature(inputs, outputs);
                } else {
                    return new AnnotatorSignature(new ArrayList<>(), new ArrayList<>());
                }
            } catch (Exception e) {
                System.out.printf("Cannot reach endpoint trying again %d/%d...\n",tries+1,100);
            }
        }
        throw new ResourceInitializationException(new Exception("Endpoint is unreachable!"));

        // List<Class<? extends Annotation>> types = Arrays.asList(Token.class, POS.class, Stem.class, 
        //     Lemma.class, Sentiment.class, Sentence.class);
        //     // , Similarity.class, Morpheme.class, MorphologicalFeatures.class, VaderSentiment.class);

        // Random rand = new Random();
        // int r1 = rand.nextInt(types.size());
        // int r2 = rand.nextInt(types.size());
        // int r3 = rand.nextInt(types.size());  
        // while (r2 == r1) {r2 = rand.nextInt(types.size());}
        // while (r3 == r1 || r3 == r2) {r3 = rand.nextInt(types.size());}

        // final AnnotatorSignature[] ann = new AnnotatorSignature[1];
        // if (rand.nextInt(types.size()) < 5) {
        //     ann[0] = new AnnotatorSignature(
        //         Arrays.asList(types.get(r1)),
        //         Arrays.asList(types.get(r2)));
        // } else {
        //     ann[0] = new AnnotatorSignature(
        //         Arrays.asList(types.get(r1), types.get(r3)),
        //         Arrays.asList(types.get(r2)));
        // }

        
        // while( currentSignatures.keySet().stream()
        //         .anyMatch(sig -> ann[0].compareSignatures(sig) == -2 || 
        //             ann[0].equals(sig))) {
        //     r1 = rand.nextInt(types.size());
        //     r2 = rand.nextInt(types.size());   
        //     r3 = rand.nextInt(types.size());   
        //     while (r2 == r1) {r2 = rand.nextInt(types.size());}
        //     while (r3 == r1 || r3 == r2) {r3 = rand.nextInt(types.size());}
            
        //     if (rand.nextInt(types.size()) < 5) {
        //         ann[0] = new AnnotatorSignature(
        //             Arrays.asList(types.get(r1)),
        //             Arrays.asList(types.get(r2)));
        //     } else {
        //         ann[0] = new AnnotatorSignature(
        //             Arrays.asList(types.get(r1), types.get(r3)),
        //             Arrays.asList(types.get(r2)));
        //     }
                
        // }

        // currentSignatures.put(ann[0], "null");

        // return ann[0];
    }

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

    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = queue.getValue0().getCommunicationLayer();
        long serializeStart = System.nanoTime();

        ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);

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

        byte[] ok = out.toByteArray();
        long sizeArray = ok.length;
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;
        int tries = 0;
        HttpResponse<byte[]> resp = null;
        while(tries < 10) {
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
                e.printStackTrace();
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
                synchronized(viewJc) {
                    layer.deserialize(viewJc, st);
                }
            }
            catch(Exception e) {
                System.err.printf("Caught exception printing response %s\n",new String(resp.body(), StandardCharsets.UTF_8));
                throw e;
            }
            long deserializeEnd = System.nanoTime();

            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc);

            comp.addComponent(queue.getValue0());
        } else {
            comp.addComponent(queue.getValue0());
            ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
            String responseBody = new String(st.readAllBytes(), StandardCharsets.UTF_8);
            st.close();
            throw new InvalidObjectException(String.format("Expected response 200, got %d: %s", resp.statusCode(), responseBody));
        }
    }

    public static void process_handler(JCas jc,
                                       IDUUIInstantiatedPipelineComponent comp,
                                       DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException, InterruptedException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = queue.getValue0().getCommunicationLayer();
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

        IDUUIUrlAccessible accessible = queue.getValue0();
        IDUUIConnectionHandler handler = accessible.getHandler();

        if (handler.getClass() == DUUIWebsocketAlt.class){
            JCas finalViewJc = viewJc;

            List<ByteArrayInputStream> results = handler.send(ok);

            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            ByteArrayInputStream result = null;
            try {
                
                result = layer.merge(results);
                synchronized(finalViewJc) {
                    layer.deserialize(finalViewJc, result);
                }       
            }
            catch(Exception e) {
                e.printStackTrace();
                System.err.printf("Caught exception printing response %s\n",new String(result.readAllBytes(), StandardCharsets.UTF_8));
            }

            long deserializeEnd = System.nanoTime();

            comp.addComponent(accessible);

            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc);
            comp.addComponent(accessible);
        }
    }
}
