import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WebsocketTest {
    public static List<DUUIComposer> _composers = new ArrayList<>();
    public static List<Path> getFilePathes(String dirPathInResources) {
        List<Path> paths = new ArrayList<>();
        InputStream resourcesPath = WebsocketTest.class.getClassLoader().getResourceAsStream(dirPathInResources);
        BufferedReader br = new BufferedReader(new InputStreamReader(resourcesPath));
        List<String> fileNameList = br.lines().collect(Collectors.toList());
        for (String path: fileNameList) {
            paths.add(Path.of(path));
        }
        return paths;
    }
    public static String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    public static void testWithWebsocket(String test) throws Exception {

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx)
                .withOpenConnection(false)
                .withSkipVerification(true);


        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);

        composer.addDriver(remote_driver);

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9715")
                .withScale(1).withWebsocket(true, 50).build());

        String val = test;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);



        // Run single document
        composer.run(jc,"fuchs");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));

        composer.shutdown();

    }

    public static void testSwarm(String test) throws Exception {

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx)
                .withSkipVerification(true);

        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();

        composer.addDriver(swarmDriver);

        composer.add(new DUUISwarmDriver.Component("localhost:5000/textimager-duui-spacy:0.1.2")
                .withScale(1)
                .withWebsocket(true).build());

        String val = test;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);

        // Run single document
        composer.run(jc,"fuchs");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
//        System.out.println(new String(out.toByteArray()));

        composer.shutdown();

    }

    public static void testDocker(String test) throws Exception {

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx)
                .withSkipVerification(true);

        DUUIDockerDriver driver = new DUUIDockerDriver()
                .withTimeout(10000);

        composer.addDriver(driver);

        composer.add(new DUUIDockerDriver.Component("textimager-duui-spacy:0.1.2")
                        .withWebsocket(true)
                        .withScale(1)
                         .build());

        String val = test;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);

        // Run single document
        composer.run(jc,"fuchs");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
//        System.out.println(new String(out.toByteArray()));

        composer.shutdown();

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
        composer.run(jc,"fuchs");
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        XmlCasSerializer.serialize(jc.getCas(),out);
//        System.out.println(new String(out.toByteArray()));

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

    public static void main(String[] args) throws Exception {

        InputStream inputStream = WebsocketTest.class.getResourceAsStream("/sample_splitted/sample_01_140.txt");
//        InputStream inputStream = WebsocketTest.class.getResourceAsStream("/sample_splitted/sample_02_349.txt");
        String text = readFromInputStream1(inputStream);

        System.out.println(text);

        //testDocker(text);
        testWithWebsocket(text);


        testWithWebsocket(text);
        DUUIComposer._clients.forEach(IDUUIConnectionHandler::close);


//        testWithWebsocket(text);
//        testWithWebsocket(text);



//        System.out.println(getFilePathes("zip"));
//        for (Path path: getFilePathes("zip")) {
//            String link = "/zip/"+path;
//            System.out.println(link);
//            InputStream inputStream = WebsocketTest.class.getResourceAsStream(link);
//            String text = readFromInputStream(inputStream);
//            System.out.println(CollectionReaderFactory.createReaderDescription(XmiReader.class,
//                    XmiReader.PARAM_LANGUAGE,"de",
//                    XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
//                    XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
//                    XmiReader.PARAM_LENIENT,true,
//                    XmiReader.PARAM_SOURCE_LOCATION,link));
//        }

    }
}
