package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIUrlAccessible;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public class SlurmProcess {
    public static HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofSeconds(1000)).build();

    /**
     * Calling the DUUI component
     * @param jc
     * @param comp
     * @param perf
     * @throws CompressorException
     * @throws IOException
     * @throws SAXException
     * @throws CASException
     */
    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException, CASException {



        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();
// get lua script
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

        layer.serialize(viewJc,out,comp.getParameters(), comp.getSourceView());
        // lua serialize call()

        byte[] ok = out.toByteArray();
        long sizeArray = ok.length;
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;
        int tries = 0;
        HttpResponse<byte[]> resp = null;
        while (tries < 3) {
            tries++;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                        .timeout(Duration.ofSeconds(comp.getPipelineComponent().getTimeout()))
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
            throw new IOException("Could not reach endpoint after 3 tries!");
        }


        if (resp.statusCode() == 200) {
            ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            try {
                layer.deserialize(viewJc, st, comp.getTargetView());
            }
            catch(Exception e) {
                System.err.printf("Caught exception printing response %s\n",new String(resp.body(), StandardCharsets.UTF_8));

                // TODO better error handleing flow?
                comp.addComponent(queue.getValue0());

                // TODO handle error docs for db here too?

                throw e;
            }
            long deserializeEnd = System.nanoTime();

            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, null);

            comp.addComponent(queue.getValue0());
        } else {
            comp.addComponent(queue.getValue0());
            ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
            String responseBody = new String(st.readAllBytes(), StandardCharsets.UTF_8);
            st.close();

            // track "performance" of error documents if not explicitly disabled
            if (perf.shouldTrackErrorDocs()) {
                long annotatorEnd = System.nanoTime();
                long deserializeStart = annotatorEnd;
                long deserializeEnd = System.nanoTime();

                String error = "Expected response 200, got " + resp.statusCode() + ": " + responseBody;

                perf.addData(serializeEnd - serializeStart, deserializeEnd - deserializeStart, annotatorEnd - annotatorStart, queue.getValue2() - queue.getValue1(), deserializeEnd - queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, error);
            }

            if (!pipelineComponent.getIgnoringHTTP200Error()) {
                throw new InvalidObjectException(String.format("Expected response 200, got %d: %s", resp.statusCode(), responseBody));
            } else {
                System.err.println(String.format("Expected response 200, got %d: %s", resp.statusCode(), responseBody));
            }
        }
    }

    /**
     * The process merchant describes the use of the component as a web socket
     * @param jc
     * @param comp
     * @param perf
     * @throws CompressorException
     * @throws IOException
     * @throws SAXException
     * @throws CASException
     * @throws InterruptedException
     */
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
        layer.serialize(viewJc,out,comp.getParameters(), comp.getSourceView());

        // ok is the message.
        byte[] ok = out.toByteArray();
        long sizeArray = ok.length;
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;

        /**
         * @edited Givara Ebo, Dawit Terefe
         *
         * Retrieve websocket-client from IDUUIUrlAccessible (ComponentInstance).
         *
         */
        IDUUIUrlAccessible accessible = queue.getValue0();
        IDUUIConnectionHandler handler = accessible.getHandler();

        if (handler.getClass() == DUUIWebsocketAlt.class){
            String error = null;

            JCas finalViewJc = viewJc;

            List<ByteArrayInputStream> results = handler.send(ok);

            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;

            ByteArrayInputStream result = null;
            try {
                /***
                 * @edited
                 * Givara Ebo, Dawit Terefe
                 *
                 * Merging results before deserializing.
                 */
                result = layer.merge(results);
                layer.deserialize(finalViewJc, result, comp.getTargetView());
            }
            catch(Exception e) {
                e.printStackTrace();
                System.err.printf("Caught exception printing response %s\n",new String(result.readAllBytes(), StandardCharsets.UTF_8));

                // TODO more error handling needed?
                error = ExceptionUtils.getStackTrace(e);
            }

            long deserializeEnd = System.nanoTime();

            comp.addComponent(accessible);

            ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
            ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
            ann.setCompression(DUUIPipelineComponent.compressionMethod);
            ann.setTimestamp(System.nanoTime());
            ann.setPipelineName(perf.getRunKey());
            ann.addToIndexes();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, error);
            comp.addComponent(accessible);
        }
    }

}
