import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.GerParCorReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.GerParCorWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

public class GerParCorAnalyse {
    int iScale = 20;

    @Test
    public void spacy() throws Exception {


        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);


        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withOverlap(500);

        composer.add(spacy.withSegmentationStrategy(pStrategy));

//        composer.add(component);


        AnalysisEngineDescription writerEngine = createEngineDescription(GerParCorWriter.class,
                GerParCorWriter.PARAM_DBConnection, "DockerUnifiedUIMAInterface/gerparcor"
        );
        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

        CollectionReaderDescription reader = createReaderDescription(GerParCorReader.class,
                GerParCorReader.PARAM_DBConnection, "DockerUnifiedUIMAInterface/gerparcor",
                GerParCorReader.PARAM_Query, "{\"annotations.Token\": 0}"
        );

        composer.run(reader);

    }

    @Test
    public void sentiment() throws Exception {


        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);


        DUUIDockerDriver.Component sentiment = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.2")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);

        composer.add(sentiment.withSegmentationStrategy(pStrategy));

//        composer.add(component);


        AnalysisEngineDescription writerEngine = createEngineDescription(GerParCorWriter.class,
                GerParCorWriter.PARAM_DBConnection, "DockerUnifiedUIMAInterface/gerparcor"
        );
        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

        CollectionReaderDescription reader = createReaderDescription(GerParCorReader.class,
                GerParCorReader.PARAM_DBConnection, "DockerUnifiedUIMAInterface/gerparcor"
        );

        composer.run(reader);

    }

}
