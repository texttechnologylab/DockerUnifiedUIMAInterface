package correctness_test;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;


class CorrectnessTest {

    public static List<DUUIComposer> _composers = new ArrayList<>();
    public static List<Path> getFilePathes(String dirPathInResources) {
        List<Path> paths = new ArrayList<>();
        InputStream resourcesPath = CorrectnessTest.class.getClassLoader().getResourceAsStream(dirPathInResources);
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

    public static String testWithWebsocket(String test) throws Exception {

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

        String val = test;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);



        // Run single document
        composer.run(jc,"fuchs");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));
        String result = new String(out.toByteArray());

        composer.shutdown();
        return result;

    }

    public static String testWithRest(String test) throws Exception {

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
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));
        String result = new String(out.toByteArray());

        composer.shutdown();
        return result;
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
    void correctness() throws Exception {
        InputStream inputStream = CorrectnessTest.class.getResourceAsStream("/sample_splitted/sample_02_349.txt");
        String text = readFromInputStream1(inputStream);
        String wsText = testWithWebsocket(text).replaceAll("<duui:ReproducibleAnnotation.*/>", "")
                .replaceAll("timestamp=\"[0-9]*\"", "0");
        String restText = testWithRest(text).replaceAll("<duui:ReproducibleAnnotation.*/>", "")
                .replaceAll("timestamp=\"[0-9]*\"", "0");
        assertEquals(restText, wsText);
    }

    public static void main(String[] args) throws Exception {
//        InputStream inputStream = WebsocketTest.class.getResourceAsStream("/sample_splitted/sample_140.txt");
        InputStream inputStream = CorrectnessTest.class.getResourceAsStream("/sample_splitted/sample_02_349.txt");
        String text = readFromInputStream1(inputStream);
        System.out.println(text);

        testWithWebsocket(text);
//        testWithRest(text);



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
