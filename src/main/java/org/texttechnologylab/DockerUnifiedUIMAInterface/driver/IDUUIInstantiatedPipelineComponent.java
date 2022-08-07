package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import io.socket.client.Ack;
import io.socket.client.Socket;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public interface IDUUIInstantiatedPipelineComponent {
    public static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofSeconds(1000)).build();

    public IDUUICommunicationLayer getCommunicationLayer();
    public DUUIPipelineComponent getPipelineComponent();
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

    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = comp.getCommunicationLayer();
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

        layer.serialize(viewJc,out,comp.getParameters());
        // lua serialize call()

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
                layer.deserialize(viewJc, st);
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
            throw new InvalidObjectException("Response code != 200, error");
        }
    }

    public static void process_handler(JCas jc,
                                       IDUUIInstantiatedPipelineComponent comp,
                                       DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException, URISyntaxException, InterruptedException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = comp.getCommunicationLayer();
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

        // ok ist message.
        byte[] ok = out.toByteArray();
        long sizeArray = ok.length;
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;

        /***
         * @edited
         * Givara Ebo
         * Installation
         */
//        handler.initiate(uri);
        /**
         * send a message with Socket
         * an Dawit
         * du kannst es noch mal freischalten.
         */

        IDUUIUrlAccessible accessible = queue.getValue0();
        IDUUIConnectionHandler handler = accessible.getHandler();
        if (handler.getClass() == DUUIWebsocketAlt.class){
            JCas finalViewJc = viewJc;

            byte[] result = handler.get(ok);

            ByteArrayInputStream st = new ByteArrayInputStream(result);

            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            try {
                /***
                 * @edited
                 * Givara Ebo
                 * ich habe es auskommentiert, um zu testen
                 * now
                 */
                /** @see **/
                layer.deserialize(finalViewJc, st);
            }
            catch(Exception e) {
                System.err.printf("Caught exception printing response %s\n",new String(result, StandardCharsets.UTF_8));
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




























        else if (handler.getClass() == DUUIWebsocketHandler.class) {
            JCas finalViewJc = viewJc;

            System.out.println("[DUUIWebsocketHandler]: Message sending \n"+
                    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ok)));

            Socket client = (Socket) handler.getClient();


            client.emit("json", ok, (Ack) objects -> {

                System.out.println("[DUUIWebsocketHandler]: Message received "+
                        StandardCharsets.UTF_8.decode(ByteBuffer.wrap((byte[]) objects[0])));


                byte[] sioresult = (byte[]) objects[0];
                ByteArrayInputStream st = new ByteArrayInputStream(sioresult);

                try {
                    /***
                     * @edited
                     * Givara Ebo
                     * ich habe es auskommentiert, um zu testen
                     * now
                     */
                    long annotatorEnd = System.nanoTime();
                    long deserializeStart = annotatorEnd;

                    layer.deserialize(finalViewJc, st);

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
                catch(Exception e) {
                    System.err.printf("Caught exception printing response %s\n",new String(sioresult, StandardCharsets.UTF_8));
                }



            });


        }
        else {

            System.out.println("[SocketIO]: SocketIO is not active");
            System.out.println("[SocketIO]: Message is not sent");
            /*
            byte[] result = handler.sendAwaitResponse(ok);
            comp.addComponent(accessible);



            ByteArrayInputStream st = new ByteArrayInputStream(result);
            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            try {
                /***
                 * @edited
                 * Givara Ebo
                 * ich habe es auskommentiert, um zu testen
                 * now
                 *
                layer.deserialize(viewJc, st);
            }
            catch(Exception e) {
                System.err.printf("Caught exception printing response %s\n",new String(result, StandardCharsets.UTF_8));
            }
            long deserializeEnd = System.nanoTime();

            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc);
            comp.addComponent(accessible);
            */
        }

    }
}
