import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.TTLabXmiWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotation;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestDUUISegmentation {

    static public void main(String[] args) throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        int iScale = 40;

        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(dockerDriver, remoteDriver);

        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4").withScale(iScale).withImageFetching();
        composer.add(component);

//        DUUISegmentationStrategy pStrategy = null;
        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByAnnotation()
                .withSegmentationClass(Sentence.class)
                .withPrintStatistics(true)
                .withMaxAnnotationsPerSegment(1000)
                .withMaxCharsPerSegment(10000);

        composer.add(component.withSegmentationStrategy(pStrategy));

        AnalysisEngineDescription writerEngine = createEngineDescription(TTLabXmiWriter.class,
                TTLabXmiWriter.PARAM_TARGET_LOCATION, "/tmp/bundestag/",
                TTLabXmiWriter.PARAM_PRETTY_PRINT, true,
                TTLabXmiWriter.PARAM_OVERWRITE, true,
                TTLabXmiWriter.PARAM_VERSION, "1.1",
                TTLabXmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

        AsyncCollectionReader dataReader = new AsyncCollectionReader("/storage/projects/abrami/GerParCor/xmi", "xmi.gz", 1, true);


        CollectionReaderDescription reader = CollectionReaderFactory.createReaderDescription(XmiReader.class
                , XmiReader.PARAM_LANGUAGE, "de"
                , XmiReader.PARAM_SOURCE_LOCATION, "/storage/xmi/GerParCorDownload/Berlin/xmi/**/*.xmi.gz"
        );

        long startTime = System.nanoTime();
//        composer.run(tCas);
//        composer.run(reader);
        composer.run(reader, "segment");
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;  // divide by 1000000 to get milliseconds.
        System.out.println("-> " + duration + "ms");

        composer.shutdown();
    }
}
