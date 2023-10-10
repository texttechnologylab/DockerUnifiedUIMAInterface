import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.AnnotationCommentsRemover;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.SimpleSegmenter;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class GerParCor {

    @Test
    public void process() throws Exception {

        int iScale = 4;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("gerparcorReloaded.db")
                .withConnectionPoolSize(1);
        composer.withStorageBackend(sqlite);

        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(dockerDriver, remoteDriver, uimaDriver);

//        DUUIDockerDriver.Component componentSentence = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-sentencizer:0.2.1").withScale(iScale).withImageFetching();
        DUUIUIMADriver.Component componentSentence = new DUUIUIMADriver.Component(
                createEngineDescription(SimpleSegmenter.class,
                        SimpleSegmenter.PARAM_SENTENCE_LENGTH, "50000")
        ).withScale(iScale);

        DUUIUIMADriver.Component pseudoremover = new DUUIUIMADriver.Component(
                createEngineDescription(AnnotationCommentsRemover.class,
                        AnnotationCommentsRemover.PARAM_ANNOTATION_KEY, "PSEUDOSENTENCE")
        ).withScale(iScale);

        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4").withScale(iScale).withImageFetching();

//        DUUISegmentationStrategy pStrategy = null;
        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(300000);

//        composer.add(componentSentence);

        composer.add(component.withSegmentationStrategy(pStrategy));

//        composer.add(pseudoremover);

//        DUUIDockerDriver.Component sentimentComponent = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.2").withScale(iScale)
//                .withParameter("model_name", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
//                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence");
//
//        composer.add(sentimentComponent.withSegmentationStrategy(pStrategy));

//        DUUIRemoteDriver.Component topicComponent = new DUUIRemoteDriver.Component("http://warogast.hucompute.org:9000", "http://warogast.hucompute.org:9001").withScale(2)
//                .withParameter("model_name", "chkla/parlbert-topic-german")
//                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence");
//
//        composer.add(topicComponent.withSegmentationStrategy(pStrategy));
//
//        DUUIRemoteDriver.Component srlComponent = new DUUIRemoteDriver.Component("http://141.2.108.253:9000", "http://141.2.108.253:9001").withScale(2);
//
//        composer.add(srlComponent.withSegmentationStrategy(pStrategy));

        AnalysisEngineDescription writerEngine = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/GerParCor/xmi",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        AsyncCollectionReader dataReader = new AsyncCollectionReader("/home/gabrami/Downloads/GerParCorTest/sentence", "xmi.gz", 1, true);
        AsyncCollectionReader dataReader = new AsyncCollectionReader("/storage/projects/abrami/GerParCor/xmi", "xmi.gz", 1, false, "/tmp/GerParCor/xmi");

        composer.run(dataReader, "reloaded");

        composer.shutdown();

    }

}
