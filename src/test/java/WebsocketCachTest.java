import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class WebsocketCachTest {
    private static int iWorkers = 1;
    private static int fileCounter = 4;


    @Test
    void testWithWebsocket(String text, String name) throws Exception {
        fileCounter+=1;
        String token_numbers = "0050"; // muss in 4 Ziffern sein
        //DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("performance_dbs/cach"+fileCounter+".db")
          //      .withConnectionPoolSize(iWorkers);
        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
            //    .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);


        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        composer.add(new DUUIRemoteDriver.Component("http://10.79.22.241:9715")
                .withScale(iWorkers).withWebsocket(true).build());
//        composer.add(new SocketIO("http://127.0.0.1:9715"));

        String val = text;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);



        // Run single document
        composer.run(jc,name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        // System.out.println(new String(out.toByteArray()));

        composer.shutdown();
    }
    public static String readFromInputStream1(InputStream inputStream)
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

    @Test
    void cachTest() throws Exception {
        InputStream inputStream = WebsocketTest.class.getResourceAsStream("/sample_splitted/sample_01_140.txt");
        String text = readFromInputStream1(inputStream);
        System.out.println(text);
        //testWithWebsocket(text, "cachTest");
        testWithWebsocket(text, "cachTest");
    }



}