import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIGerParCorReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer.GerParCorWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class Corpora {

    @Test
    public void C4() throws Exception {

        int iScale = 20;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        composer.addDriver(uimaDriver, dockerDriver);

        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(spacy);

//        composer.add(component);

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/corpora/c4/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());

        String sourcePath = "/storage/projects/Verben/c4";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, ".xmi.gz", 1, false);

        composer.run(collectionReader, "spacy");

    }

    @Test
    public void Twitter() throws Exception {

        int iScale = 20;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        composer.addDriver(uimaDriver, dockerDriver);

        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(spacy);

//        composer.add(component);

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/corpora/twitter_sample/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());

        String sourcePath = "/storage/projects/Verben/twitter_sample/xmi";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, ".xmi.gz", 1, false);

        composer.run(collectionReader, "spacy");

    }

}
