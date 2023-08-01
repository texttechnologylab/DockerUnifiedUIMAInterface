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
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.BorlandExport;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.ChangeMetaData;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.RemoveMetaData;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class Experiments {

    @Test
    public void ChatGPT_Verbs() throws Exception {

        String sourcePath = "/storage/projects/abrami/verbs/xmi";
        String outputPath = "/storage/projects/abrami/verbs/output";
        String sourceSuffix = ".xmi";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, true);

        int iWorkers = Integer.valueOf(30);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("verbs" + iWorkers+ ".db")
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

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(ChangeMetaData.class))
                .withScale(iWorkers));

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers).withImageFetching());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveMetaData.class))
                .withScale(iWorkers));

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, outputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, false,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(collectionReader, "chat_gpt_"+iWorkers);

    }

    @Test
    public void ChatGPT_Sum() throws Exception {

        String sourcePath = "/storage/projects/abrami/verbs/output";
        String sourceSuffix = ".xmi.gz";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, true);

        int iWorkers = Integer.valueOf(40);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("verbs_export" + iWorkers+ ".db")
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

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BorlandExport.class)).build());

        composer.run(collectionReader, "chat_gpt_output_"+iWorkers);

    }

}
