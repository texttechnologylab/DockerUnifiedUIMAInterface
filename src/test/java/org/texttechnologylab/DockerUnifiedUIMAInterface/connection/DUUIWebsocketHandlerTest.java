package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.junit.jupiter.api.Test;
import org.luaj.vm2.ast.Str;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class IDUUIConnectionHandlerTest {
    private static int iWorkers = 1;
    private static String sourceLocation = "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed/*.xmi";

    //InputStream inputStream = IDUUIConnectionHandlerTest.class.getResourceAsStream("/sample_splitted/sample_140.txt");
    //String text = readFromInputStream(inputStream);

    IDUUIConnectionHandlerTest() throws IOException {
    }
    @Test
    void testWithWebsocket(String text, String name) throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("websocket_rest_test1.db")
                .withConnectionPoolSize(iWorkers);
        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);


        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715")
                .withScale(iWorkers).withWebsocket(true).build());
//        composer.add(new SocketIO("http://127.0.0.1:9715"));

        String val = text;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);



        // Run single document
        composer.run(jc,(name+"_ws"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));

        composer.shutdown();
    }
    @Test
    void testWithRest(String text, String name) throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("websocket_rest_test1.db")
                .withConnectionPoolSize(iWorkers);
        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);


        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715")
                .withScale(iWorkers).withWebsocket(false).build());
//        composer.add(new SocketIO("http://127.0.0.1:9715"));

        String val = text;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);



        // Run single document
        composer.run(jc,name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));

        composer.shutdown();
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

    public static List<Path> getFilePathes(String dirPathInResources) {
        List<Path> paths = new ArrayList<>();
        InputStream resourcesPath = IDUUIConnectionHandlerTest.class.getClassLoader().getResourceAsStream(dirPathInResources);
        BufferedReader br = new BufferedReader(new InputStreamReader(resourcesPath));
        List<String> fileNameList = br.lines().collect(Collectors.toList());
        for (String path: fileNameList) {
            paths.add(Path.of(path));
        }
        return paths;
    }

    @Test
    void forRestTest() throws Exception {
        for (Path path: getFilePathes("sample_splitted")) {
            String link = "/sample_splitted/"+path;
            System.out.println(link);
            InputStream inputStream = IDUUIConnectionHandlerTest.class.getResourceAsStream(link);
            String text = readFromInputStream(inputStream);
            testWithRest(text, String.valueOf(path));
        }

    }
    @Test
    void forWebsocketTest() throws Exception {
        for (Path path: getFilePathes("sample_splitted")) {
            String link = "/sample_splitted/"+path;
            System.out.println(link);
            InputStream inputStream = IDUUIConnectionHandlerTest.class.getResourceAsStream(link);
            String text = readFromInputStream(inputStream);
            testWithWebsocket(text, String.valueOf(path));
        }

    }




}