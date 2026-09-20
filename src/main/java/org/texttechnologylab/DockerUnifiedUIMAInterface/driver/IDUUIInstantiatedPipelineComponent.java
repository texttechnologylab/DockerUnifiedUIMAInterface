package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIComponentLog;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.io.*;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The interface for the instance of each component that is executed in a pipeline.
 *
 * @author Alexander Leonhardt
 */
public interface IDUUIInstantiatedPipelineComponent {
    HttpClient _client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .proxy(ProxySelector.getDefault())
            .connectTimeout(Duration.ofSeconds(1000)).build();
    int postTries = 50;

    /**
     * Returns the TypeSystem used for the DUUI component used.
     *
     * @param uuid
     * @param comp
     * @return
     * @throws ResourceInitializationException
     */
    static TypeSystemDescription getTypesystem(String uuid, IDUUIInstantiatedPipelineComponent comp) throws ResourceInitializationException {
        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();
        //System.out.printf("Address %s\n",queue.getValue0().generateURL()+ DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM);

        int tries = 0;
        while (tries < postTries) {
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
                    tmp.deleteOnExit();
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
                System.out.printf("Cannot reach endpoint trying again %d/%d...\n", tries + 1, postTries);
                try {
                    Thread.sleep(comp.getPipelineComponent().getTimeout());
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        throw new ResourceInitializationException(new Exception("Endpoint is unreachable!"));
    }

    /**
     * Calling the DUUI component
     *
     * @param jc
     * @param comp
     * @param perf
     * @throws CASException
     * @throws PipelineComponentException
     */
    static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CASException, PipelineComponentException {
        process(jc, comp, perf, null);
    }

    /**
     * Calling the DUUI component. When a {@code composer} is given and component logging is
     * enabled, the tool is asked to return its logs on the {@code /v1/process} response
     * (piggyback) via the {@code DUUI-Log-Collect} header.
     *
     * @param jc       the CAS to process
     * @param comp     the instantiated component
     * @param perf     the performance tracker
     * @param composer the owning composer (may be {@code null}; then no log headers are added)
     */
    static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException {
        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = queue.getValue0().getCommunicationLayer();
        long serializeStart = System.nanoTime();

        try {
            // Route DUUILogging:log*() calls to this composer, tagged with the component and document
            if (layer instanceof DUUILuaCommunicationLayer luaLayer
                    && composer != null && composer.isComponentLoggingEnabled()) {
                luaLayer.setLogContext(composer, safeComponentKey(comp), safeComponentName(comp), safeDocumentId(jc), perf);
            }

            DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();
            String viewName = pipelineComponent.getViewName();
            JCas viewJc;
            if (viewName == null) {
                viewJc = jc;
            } else {
                try {
                    viewJc = jc.getView(viewName);
                } catch (CASException e) {
                    if (pipelineComponent.getCreateViewFromInitialView()) {
                        viewJc = jc.createView(viewName);
                        viewJc.setDocumentText(jc.getDocumentText());
                        viewJc.setDocumentLanguage(jc.getDocumentLanguage());
                    } else {
                        throw e;
                    }
                }
            }

            if (layer.supportsProcess()) {
                JCas sourceCas = viewJc.getView(comp.getSourceView());
                JCas targetCas;
                try {
                    targetCas = viewJc.getView(comp.getTargetView());
                } catch (CASException e) {
                    targetCas = viewJc.createView(comp.getTargetView());
                }

                DUUIHttpRequestHandler processHandler =
                        new DUUIHttpRequestHandler(_client, queue.getValue0().generateURL(), pipelineComponent.getTimeout());
                applyLogHeaders(processHandler, jc, comp, composer);
                layer.process(
                        sourceCas,
                        processHandler,
                        comp.getParameters(),
                        targetCas
                );

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();

                return;
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024);

            // Invoke Lua serialize()
            layer.serialize(viewJc, out, comp.getParameters(), comp.getSourceView());

            byte[] ok = out.toByteArray();
            long sizeArray = ok.length;
            long serializeEnd = System.nanoTime();

            long annotatorStart = serializeEnd;
            int tries = 0;
            HttpResponse<byte[]> resp = null;
            boolean bRunning = true;
            while (bRunning) {
                tries++;
                try {
                    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                            .uri(URI.create(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                            .timeout(Duration.ofSeconds(comp.getPipelineComponent().getTimeout()))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(ok))
                            .version(HttpClient.Version.HTTP_1_1);
                    applyLogHeaders(requestBuilder, jc, comp, composer);
                    HttpRequest request = requestBuilder.build();
                    resp = _client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                    break;
                } catch (Exception e) {
                    //                e.printStackTrace();
                    System.out.printf("Cannot reach endpoint trying again %d/%d...\n", tries + 1, postTries);
                    try {
                        Thread.sleep(comp.getPipelineComponent().getTimeout());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    if (tries > postTries) {
                        bRunning = false;
                    }
                }
            }
            if (resp == null) {
                throw new IOException("Could not reach endpoint after " + postTries + " tries!");
            }


            // Piggybacked component logs, present when DUUI-Log-Collect was sent. Collected
            // for both success and error responses (logs are most useful on failures).
            collectResponseLogs(resp.headers().firstValue(HEADER_LOGS).orElse(null), jc, comp, composer, perf);

            if (resp.statusCode() == 200) {
                ByteArrayInputStream st = new ByteArrayInputStream(resp.body());
                long annotatorEnd = System.nanoTime();
                long deserializeStart = annotatorEnd;

                layer.deserialize(viewJc, st, comp.getTargetView());

                long deserializeEnd = System.nanoTime();

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();
                perf.addData(serializeEnd - serializeStart, deserializeEnd - deserializeStart, annotatorEnd - annotatorStart, queue.getValue2() - queue.getValue1(), deserializeEnd - queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, null);

            } else {
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
                    System.err.printf("Expected response 200, got %d: %s%n", resp.statusCode(), responseBody);
                }
            }
        } catch (CASException e) {
            throw e;
        } catch (Exception e) {
            try {
                DocumentMetaData documentMetaData = DocumentMetaData.get(jc);
                throw new PipelineComponentException(comp.getPipelineComponent(), documentMetaData, e);
            } catch (IllegalArgumentException ignored) {
                throw new PipelineComponentException(comp.getPipelineComponent(), e);
            }
        } finally {
            if (layer instanceof DUUILuaCommunicationLayer luaLayer) {
                luaLayer.clearLogContext();
            }
            comp.addComponent(queue.getValue0());
        }
    }

    /**
     * Request header asking the tool to return its logs on the response (piggyback).
     * The tool's {@code duui_logging} middleware only collects/returns logs when this is set.
     */
    String HEADER_LOG_COLLECT = "DUUI-Log-Collect";
    /** Response header carrying the tool's logs as a JSON array. */
    String HEADER_LOGS = "DUUI-Logs";

    /**
     * Ask the tool to return its logs, if the composer has component logging enabled, by
     * setting the {@link #HEADER_LOG_COLLECT} request header. No-op when {@code composer} is
     * {@code null} or logging is disabled.
     */
    static void applyLogHeaders(DUUIHttpRequestHandler handler, JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIComposer composer) {
        if (composer != null && composer.isComponentLoggingEnabled()) {
            handler.header(HEADER_LOG_COLLECT, "true");
        }
    }

    /**
     * Ask the tool to return its logs, if the composer has component logging enabled, by
     * setting the {@link #HEADER_LOG_COLLECT} request header on a raw {@link HttpRequest.Builder}.
     */
    static void applyLogHeaders(HttpRequest.Builder builder, JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIComposer composer) {
        if (composer != null && composer.isComponentLoggingEnabled()) {
            builder.header(HEADER_LOG_COLLECT, "true");
        }
    }

    /**
     * Read the {@link #HEADER_LOGS} response header (if present) and route each log record to
     * the composer, tagged with the component and document. No-op when logging is off or the
     * header is absent.
     *
     * @param responseLogsHeader the {@code DUUI-Logs} response header value, or {@code null}
     * @param perf               the per-document performance tracker the logs are also appended
     *                           to for the "batch per document" DB path, may be {@code null}
     */
    static void collectResponseLogs(String responseLogsHeader, JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIComposer composer, DUUIPipelineDocumentPerformance perf) {
        if (composer == null || responseLogsHeader == null || responseLogsHeader.isEmpty()) {
            return;
        }
        DUUIComponentLog.emit(composer, safeComponentKey(comp), safeComponentName(comp), safeDocumentId(jc), responseLogsHeader, perf);
    }

    private static String safeComponentKey(IDUUIInstantiatedPipelineComponent comp) {
        try {
            return comp.getUniqueComponentKey();
        } catch (Exception e) {
            return null;
        }
    }

    private static String safeComponentName(IDUUIInstantiatedPipelineComponent comp) {
        try {
            return comp.getPipelineComponent().getName();
        } catch (Exception e) {
            return null;
        }
    }

    private static String safeDocumentId(JCas jc) {
        try {
            DocumentMetaData dmd = DocumentMetaData.get(jc);
            return dmd == null ? null : dmd.getDocumentId();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * The process merchant describes the use of the component as a web socket
     *
     * @param jc
     * @param comp
     * @param perf
     * @throws CASException
     * @throws PipelineComponentException
     */
    static void process_handler(JCas jc,
                                IDUUIInstantiatedPipelineComponent comp,
                                DUUIPipelineDocumentPerformance perf) throws CASException, PipelineComponentException {
        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();

        /**
         * @edited Givara Ebo, Dawit Terefe
         *
         * Retrieve websocket-client from IDUUIUrlAccessible (ComponentInstance).
         *
         */
        IDUUIUrlAccessible accessible = queue.getValue0();
        IDUUIConnectionHandler handler = accessible.getHandler();

        IDUUICommunicationLayer layer = queue.getValue0().getCommunicationLayer();
        long serializeStart = System.nanoTime();

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            DUUIPipelineComponent pipelineComponent = comp.getPipelineComponent();

            String viewName = pipelineComponent.getViewName();
            JCas viewJc;
            if (viewName == null) {
                viewJc = jc;
            } else {
                try {
                    viewJc = jc.getView(viewName);
                } catch (CASException e) {
                    if (pipelineComponent.getCreateViewFromInitialView()) {
                        viewJc = jc.createView(viewName);
                        viewJc.setDocumentText(jc.getDocumentText());
                        viewJc.setDocumentLanguage(jc.getDocumentLanguage());
                    } else {
                        throw e;
                    }
                }
            }
            // lua serialize call()
            layer.serialize(viewJc, out, comp.getParameters(), comp.getSourceView());

            // ok is the message.
            byte[] ok = out.toByteArray();
            long sizeArray = ok.length;
            long serializeEnd = System.nanoTime();

            long annotatorStart = serializeEnd;

            if (handler.getClass() == DUUIWebsocketAlt.class) {
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
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.printf("Caught exception printing response %s\n", new String(result.readAllBytes(), StandardCharsets.UTF_8));

                    // TODO more error handling needed?
                    error = ExceptionUtils.getStackTrace(e);
                }

                long deserializeEnd = System.nanoTime();

                ReproducibleAnnotation ann = new ReproducibleAnnotation(jc);
                ann.setDescription(comp.getPipelineComponent().getFinalizedRepresentation());
                ann.setCompression(DUUIPipelineComponent.compressionMethod);
                ann.setTimestamp(System.nanoTime());
                ann.setPipelineName(perf.getRunKey());
                ann.addToIndexes();
                perf.addData(serializeEnd - serializeStart, deserializeEnd - deserializeStart, annotatorEnd - annotatorStart, queue.getValue2() - queue.getValue1(), deserializeEnd - queue.getValue1(), String.valueOf(comp.getPipelineComponent().getFinalizedRepresentationHash()), sizeArray, jc, error);
            }
        } catch (CASException e) {
            throw e;
        } catch (Exception e) {
            try {
                DocumentMetaData documentMetaData = DocumentMetaData.get(jc);
                throw new PipelineComponentException(comp.getPipelineComponent(), documentMetaData, e);
            } catch (IllegalArgumentException ignored) {
                throw new PipelineComponentException(comp.getPipelineComponent(), e);
            }
        } finally {
            comp.addComponent(accessible);
        }
    }

    DUUIPipelineComponent getPipelineComponent();

    Triplet<IDUUIUrlAccessible, Long, Long> getComponent();

    void addComponent(IDUUIUrlAccessible item);

    Map<String, String> getParameters();

    String getSourceView();

    String getTargetView();

    String getUniqueComponentKey();
}
