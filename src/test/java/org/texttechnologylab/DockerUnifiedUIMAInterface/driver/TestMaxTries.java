package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import com.sun.net.httpserver.HttpServer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link DUUIPipelineComponent#withMaxTries(int)}: how often a document is sent to a component whose answer takes
 * longer than {@link DUUIPipelineComponent#withTimeout(long)} (a local fake component that counts the requests).
 */
public class TestMaxTries {

    private static final String LUA = "function serialize(inputCas, outputStream, parameters)\n"
            + "  outputStream:write(json.encode({text = inputCas:getDocumentText()}))\n"
            + "end\n"
            + "function deserialize(inputCas, inputStream)\n"
            + "end\n";

    /** Fake component: answers /v1/process after {@code delayMs}, counts the process requests. */
    private static HttpServer startComponent(long delayMs, AtomicInteger processRequests) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            byte[] body;
            int status = 200;
            if (path.equals(DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER)) {
                body = LUA.getBytes(StandardCharsets.UTF_8);
            } else if (path.equals(DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS)) {
                processRequests.incrementAndGet();
                exchange.getRequestBody().readAllBytes();
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                body = "{}".getBytes(StandardCharsets.UTF_8);
            } else {
                status = 404;  // e.g. no type system: DUUI uses the default one
                body = new byte[0];
            }
            try {
                exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
                if (body.length > 0) {
                    exchange.getResponseBody().write(body);
                }
            } catch (java.io.IOException ignored) {
                // client gave up (timeout)
            } finally {
                exchange.close();
            }
        });
        server.start();
        return server;
    }

    private static void run(String url, long timeoutSeconds, Integer maxTries) throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withWorkers(1)
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());
        composer.addDriver(new DUUIRemoteDriver());
        DUUIPipelineComponent component = new DUUIRemoteDriver.Component(url).withScale(1).build()
                .withTimeout(timeoutSeconds);
        if (maxTries != null) {
            component.withMaxTries(maxTries);
        }
        composer.add(component);
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText("Ein Text.");
        jCas.setDocumentLanguage("de");
        try {
            composer.run(jCas, "maxtries");
        } finally {
            composer.shutdown();
        }
    }

    @Test
    public void slowAnswerIsSentMaxTriesTimes() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startComponent(3000, requests);
        try {
            // timeout 1 s < 3 s answer: every try times out
            assertThrows(Exception.class, () -> run("http://localhost:" + server.getAddress().getPort(), 1, 3));
            assertEquals(3, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void oneTrySendsTheDocumentOnce() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startComponent(3000, requests);
        try {
            assertThrows(Exception.class, () -> run("http://localhost:" + server.getAddress().getPort(), 1, 1));
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void answerWithinTheTimeoutIsSentOnce() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startComponent(1000, requests);
        try {
            run("http://localhost:" + server.getAddress().getPort(), 10, null);
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void maxTriesMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new DUUIPipelineComponent().withMaxTries(0));
    }
}
