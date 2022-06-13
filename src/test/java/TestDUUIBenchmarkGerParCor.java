import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

public class TestDUUIBenchmarkGerParCor {
    private static int iWorkers = 8;
    private static String sourceLocation = "/mnt/corpora2/xmi/ParliamentOutNew/";
    private static String sourceSuffix = ".xmi.gz";

//    @Test
    public void ComposerAsyncCollectionReader() throws Exception {
        AsyncCollectionReader rd = new AsyncCollectionReader(sourceLocation, sourceSuffix, 2, false);
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
                .withImageFetching());

        composer.run(rd,"async_reader_serde_echo_binary");
        composer.shutdown();
    }

    AsyncCollectionReader rd = new AsyncCollectionReader(sourceLocation, sourceSuffix, 1, 10000, false);


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
                .withScale(iWorkers));
                //.withImageFetching());

//        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
//                XmiReader.PARAM_LANGUAGE,"de",
//                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_LENIENT,true,
//                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_binary");

        composer.run(rd, "run_serde_echo_binary");
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

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_serde_echo_xmi:0.2")
                .withScale(iWorkers));
//                .withImageFetching());
//
//        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
//                XmiReader.PARAM_LANGUAGE,"de",
//                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_LENIENT,true,
//                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_xmi");
        composer.run(rd, "run_serde_echo_xmi");

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
                .withImageFetching());

//        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
//                XmiReader.PARAM_LANGUAGE,"de",
//                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_LENIENT,true,
//                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_msgpack");
        composer.run(rd, "run_serde_echo_msgpack");

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
                .withImageFetching());

//        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
//                XmiReader.PARAM_LANGUAGE,"de",
//                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
//                XmiReader.PARAM_LENIENT,true,
//                XmiReader.PARAM_SOURCE_LOCATION,sourceLocation),"run_serde_echo_json");

        composer.run(rd, "run_serde_echo_json");

        composer.shutdown();
    }
}
