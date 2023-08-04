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

public class CountExperiments {

    @Test
    public void countEmptyFiles() throws Exception {

        String sourcePath = "/storage/xmi/GerParCorDownload";
        String sourceSuffix = ".xmi.gz";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, true);

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


        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveAnnotations.class)));


        composer.run(collectionReader, "empty" + iWorkers);

    }


}
