import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.dkpro.core.io.xmi.XmiReader;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

import java.io.ByteArrayOutputStream;

public class TestDUUIBenchmark {
    private static int iWorkers = 2;
    private static String sourceLocation = "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed_test/*.xmi.gz";

    @Test
    public void ComposerPerformanceTestJava() throws Exception {
        JCas jc = JCasFactory.createJCas();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CasIOUtils.save(jc.getCas(), out, SerialFormat.XMI_1_1);
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIRemoteDriver());
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers)
                .withImageFetching()
                .build());
        //composer.add(new DUUIRemoteDriver.Component("http://localhost:9714")
        //                .withScale(1)
        //                .build());
        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_java_xmi_annotator");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestPython() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUISwarmDriver());
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_python_xmi:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_python_xmi_annotator");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestJavaToken() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_java_xmi_token:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_java_token_annotator");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestPythonToken() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_python_xmi_token:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_python_token_annotator");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestRustToken() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_rust_xmi_token:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_rust_token_annotator");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestEchoSerializeDeserializeBinary() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_serde_echo_binary:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_binary");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestEchoSerializeDeserializeXmi() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_serde_echo_xmi:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_xmi");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestEchoSerializeDeserializeMsgpack() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_serde_echo_msgpack:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_msgpack");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestEchoSerializeDeserializeJson() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization_gercorpa.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_serde_echo_json:0.1")
                        .withScale(iWorkers)
                        .withImageFetching()
                        .build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_json");
        composer.shutdown();
    }
}
