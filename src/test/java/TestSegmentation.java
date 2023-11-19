import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.RemoveAnnotations;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestSegmentation {

    @Test
    public void sample() throws Exception {

        String sourcePath = "/storage/xmi/SZ/1992-2014/TEIBasic_xmi";
        String sOutputPath = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/sz_abrami";
        String sourceSuffix = ".xmi.gz";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, 1000, AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.RANDOM, "/tmp/sampleTestSZ", false, null, 22 * 1024);

        int iWorkers = Integer.valueOf(30);

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


        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveAnnotations.class)).withScale(iWorkers).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, false,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(collectionReader, "empty" + iWorkers);

    }


}
