import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

import java.util.ArrayList;
import java.util.List;

public class TestDUUIBenchmarkGerParCor {
    private static int iWorkers = 4;
    private static String sourceLocation = "/mnt/corpora2/xmi/ParliamentOutNew/";
    private static String sourceSuffix = ".xmi.gz";
    private static int sampleSize = 100;
    private static String sLogging = "serialization_gercorpa_" + sampleSize + ".db";
    private static String sLoggingBenchmark = "swarm_benchmark_gercorpa_" + sampleSize + ".db";

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

        composer.run(rd, "async_reader_serde_echo_binary");
        composer.shutdown();
    }

    AsyncCollectionReader rd = new AsyncCollectionReader(sourceLocation, sourceSuffix, 1, sampleSize, false, "serialize_gerparcor" + sampleSize);

    @Test
    public void ComposerPerformanceTestEchoSerializeDeserializeBinary() throws Exception {
        rd.reset();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(sLogging)
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
        rd.reset();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(sLogging)
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
        rd.reset();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(sLogging)
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
        rd.reset();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(sLogging)
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


    //AsyncCollectionReader benchmarkReader = new AsyncCollectionReader(sourceLocation, sourceSuffix, 1, sampleSize, false, "/tmp/sample_benchmark_"+sampleSize);

    @Test
    public void DUUIBenchmarkSwarmTest() throws Exception {
        int iSample = 100;
        int iValue = 4;
        AsyncCollectionReader benchmarkReader = new AsyncCollectionReader(sourceLocation, sourceSuffix, 1, iSample, false, "/resources/public/abrami/evaluation_DUUI/sample_benchmark_" + iSample);

        try {
            DUUIWorker(benchmarkReader, "swarm", "benchmark_swarm_" + iValue, iWorkers, iSample);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void DUUIBenchmarkSwarm() throws Exception {

        List<Integer> iValues = new ArrayList<>();
        iValues.add(1);
        iValues.add(2);
        iValues.add(4);
        iValues.add(8);

        List<Integer> iSamples = new ArrayList<>();
        iSamples.add(100);
        iSamples.add(200);
        iSamples.add(500);
        iSamples.add(1000);

        iValues.stream().forEach(iValue -> {

            iSamples.stream().forEach(iSample -> {

                AsyncCollectionReader benchmarkReader = new AsyncCollectionReader(sourceLocation, sourceSuffix, 1, iSample, false, "/resources/public/abrami/evaluation_DUUI/sample_benchmark_" + iSample);

                try {
                DUUIWorker(benchmarkReader, "docker", "benchmark_docker_" + iValue, iValue, iSample);

                DUUIWorker(benchmarkReader, "swarm", "benchmark_swarm_"+iValue, iValue, iSample);

                } catch (Exception e) {
                    e.printStackTrace();
                }


            });


        });


    }


    public void DUUIWorker(AsyncCollectionReader reader, String sType, String sName, int iWorkers, int sampleSize) throws Exception {

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(sType+"_benchmark_gercorpa_" + sampleSize + ".db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        if(sType.equals("swarm")){
            DUUISwarmDriver swarm_driver = new DUUISwarmDriver().withSwarmVisualizer();
            composer.addDriver(swarm_driver);
        }

        DUUIDockerDriver docker_driver = new DUUIDockerDriver().withTimeout(10000);
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();


        composer.addDriver(docker_driver, remote_driver, uima_driver);

        if (sType.equals("swarm")) {
            composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                    .withScale(iWorkers).build());
        } else {
            composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                    .withScale(iWorkers).withImageFetching());
        }

//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
//                .withScale(iWorkers)
//                .build());

//        composer.add(new DUUIUIMADriver.Component(
//                createEngineDescription(XmiWriter.class,
//                        XmiWriter.PARAM_TARGET_LOCATION, "/tmp/output/",
//                        XmiWriter.PARAM_OVERWRITE, true,
//                        XmiWriter.PARAM_COMPRESSION, "GZIP"
//                )).withScale(iWorkers));

        composer.run(reader, sName);

        composer.shutdown();

    }


}
