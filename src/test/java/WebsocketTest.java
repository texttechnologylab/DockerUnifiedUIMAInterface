import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.testcontainers.shaded.org.yaml.snakeyaml.composer.Composer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class WebsocketTest {
    public static List<DUUIComposer> _composers = new ArrayList<>();
    public static void testWithWebsocket(String test) throws Exception {

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx)
                .withSkipVerification(true);


        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715")
                .withScale(1).withWebsocket(true).build());

        String val = "Dies ist ein kleiner Test Text für Abies!";
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);



        // Run single document
        composer.run(jc,"fuchs");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));

        //composer.shutdown();
        _composers.add(composer);

    }

    public static void testWithRest(String test) throws Exception {

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx)
                .withSkipVerification(true);


        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715")
                .withScale(1).withWebsocket(false).build());
//        composer.add(new SocketIO("http://127.0.0.1:9715"));

        String val = test;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);

        // Run single document
        composer.run(jc,"fuchs");
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        XmlCasSerializer.serialize(jc.getCas(),out);
//        System.out.println(new String(out.toByteArray()));

        composer.shutdown();
    }
    public static String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    public static void main(String[] args) throws Exception {
        InputStream inputStream = WebsocketTest.class.getResourceAsStream("/sample_splitted/sample_140.txt");
        String text = readFromInputStream(inputStream);
        System.out.println(text);

        testWithWebsocket(text);
        testWithWebsocket(text);
        for (DUUIComposer _compser: _composers) {
            _compser.shutdown();
        }


    }
}
