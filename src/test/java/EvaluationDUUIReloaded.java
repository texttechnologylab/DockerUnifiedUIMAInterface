import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.RemoveAnnotations;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

public class EvaluationDUUIReloaded {

    public static String sourcePath = "/storage/xmi/GerParCorDownload";
    public static String outputPath = "/storage/projects/DUUI/evaluation/source";
    public static String savePath = "/tmp/duuireloaded";
    private static String sourceSuffix = ".xmi.gz";


    @Test
    public void prepareData() throws Exception {

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, 100, false, savePath);

        int iWorkers = Integer.valueOf(4);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveAnnotations.class)).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, outputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(collectionReader, "clean");


    }

    @Test
    public void createXMIs() throws Exception {

        String sourcePath = "/storage/projects/DUUI/evaluation/gerpol/input/output";
        String outputPath = "/storage/projects/DUUI/evaluation/gerpol/xmi";

//        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, ".txt", 1, true);

        int iWorkers = Integer.valueOf(10);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, dockerDriver);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, outputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        CollectionReaderDescription reader = null;

        reader = createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, sourcePath + "/**" + ".txt",
                TextReader.PARAM_LANGUAGE, "de"
        );


        composer.run(reader, "transform_"+iWorkers);

    }

    @Test
    public void example() throws Exception {

        String sourcePath = "/storage/projects/DUUI/evaluation/source";
        String outputPath = "/storage/projects/DUUI/evaluation/target_duui";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, true);

        int iWorkers = Integer.valueOf(2);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_gercorpa_v2_" + iWorkers+ ".db")
                .withConnectionPoolSize(iWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, dockerDriver);

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers).withImageFetching());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, outputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(collectionReader, "spacy_"+iWorkers);

    }

}
