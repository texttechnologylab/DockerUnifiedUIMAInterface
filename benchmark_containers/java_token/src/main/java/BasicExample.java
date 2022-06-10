import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.xml.sax.SAXException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;

public class BasicExample {
    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(9714), 0);
        server.createContext("/v1/communication_layer", new CommunicationLayer());
        server.createContext("/v1/typesystem", new TypesystemHandler());
        server.createContext("/v1/process", new ProcessHandler());


        server.setExecutor(null); // creates a default executor
        server.start();
    }

    static class ProcessHandler implements HttpHandler {
        static JCas jc;

        static {
            try {
                TypeSystemDescription dec = TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(ProcessHandler.class.getClassLoader().getResource("token_only_types.xml").toURI().toString());
                jc = JCasFactory.createJCas(dec);
            } catch (UIMAException e) {
                e.printStackTrace();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            try {
                jc.reset();
                XmiCasDeserializer.deserialize(t.getRequestBody(),jc.getCas(),true);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                t.sendResponseHeaders(200,0);
                XmiCasSerializer.serialize(jc.getCas(),null,t.getResponseBody());
                t.getResponseBody().close();
            } catch (SAXException e) {
                e.printStackTrace();
            }
            t.sendResponseHeaders(404,-1);
            t.getResponseBody().close();
        }
    }
    static class TypesystemHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            try {
                byte[] response = Files.readAllBytes(Paths.get(ProcessHandler.class.getClassLoader().getResource("token_only_types.xml").toURI()));

                t.sendResponseHeaders(200, response.length);
                OutputStream os = t.getResponseBody();
                os.write(response);
                os.close();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

        }
    }

    static class CommunicationLayer implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String response = "serial = luajava.bindClass(\"org.apache.uima.cas.impl.XmiCasSerializer\")\n" +
                    "deserial = luajava.bindClass(\"org.apache.uima.cas.impl.XmiCasDeserializer\")\n" +
                    "function serialize(inputCas,outputStream,params)\n" +
                    "  serial:serialize(inputCas:getCas(),outputStream)\n" +
                    "end\n" +
                    "\n" +
                    "function deserialize(inputCas,inputStream)\n" +
                    "  inputCas:reset()\n" +
                    "  deserial:deserialize(inputStream,inputCas:getCas(),true)\n" +
                    "end";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
