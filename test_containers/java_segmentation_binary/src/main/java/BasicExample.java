import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasIOUtils;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;

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
                jc = JCasFactory.createJCas();
            } catch (UIMAException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            try {
                jc.reset();
                CasIOUtils.load(t.getRequestBody(),jc.getCas());
                SimplePipeline.runPipeline(jc, AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class));
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                t.sendResponseHeaders(200,0);
                CasIOUtils.save(jc.getCas(),t.getResponseBody(), SerialFormat.BINARY_TSI);
                t.getResponseBody().close();
            } catch (ResourceInitializationException e) {
                e.printStackTrace();
            } catch (AnalysisEngineProcessException e) {
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
                TypeSystemDescription desc = TypeSystemDescriptionFactory.createTypeSystemDescription();
                StringWriter writer = new StringWriter();
                desc.toXML(writer);
                String response = writer.getBuffer().toString();

                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (ResourceInitializationException e) {
                e.printStackTrace();
                t.sendResponseHeaders(404,-1);
                t.getResponseBody().close();
                return;
            } catch (SAXException e) {
                e.printStackTrace();
            }

        }
    }

    static class CommunicationLayer implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String response = "serial = luajava.bindClass(\"org.apache.uima.util.CasIOUtils\")\n" +
                    "serialtype = luajava.bindClass(\"org.apache.uima.cas.SerialFormat\");\n" +
                    "\n" +
                    "function serialize(inputCas,outputStream,params)\n" +
                    "  serial:save(inputCas:getCas(),outputStream,serialtype.BINARY_TSI)\n" +
                    "end\n" +
                    "\n" +
                    "function deserialize(inputCas,inputStream)\n" +
                    "  inputCas:reset()\n" +
                    "  serial:load(inputStream,inputCas:getCas())\n" +
                    "end";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
