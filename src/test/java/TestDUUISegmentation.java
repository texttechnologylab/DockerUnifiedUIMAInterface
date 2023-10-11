import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;

import java.util.ArrayList;
import java.util.List;

public class TestDUUISegmentation {
    static public void main(String[] args) throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(dockerDriver, remoteDriver);

        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4").withImageFetching();
//        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.6").withImageFetching();


        JCas tCas = JCasFactory.createText("Nach der Realschule absolvierte Hoch zunächst eine Kaufmännische Lehre in Danzig. Anschließend besuchte er das Gymnasium in Stolp, wo er 1885 das Abitur ablegte. Im selben Jahr nahm er ein Studium der Staatswissenschaft an der Universität zu Berlin auf, das er nach dem Militärdienst (1886/87) in Königsberg fortsetzte und 1890 an der Universität Zürich beendete. Anschließend war er als Schriftsteller und als Redakteur der Frankfurter Volksstimme in Frankfurt am Main tätig. 1895 zog er nach Hanau, wo er ein Buch- und Tabakwarengeschäft betrieb und von 1903 bis 1919 als Arbeitersekretär tätig war. Nebenberuflich war er bis 1916 Redakteur des Gewerkschaftsblattes Dachdecker-Zeitung. Er schrieb auch für die sozialdemokratische Frauenzeitschrift Die Gleichheit.", "de");

        DUUISegmentationStrategy pStrategy = null;
//        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByAnnotation()
//                .withSegmentationClass(Sentence.class)
//                .withPrintStatistics(true)
//                .withMaxAnnotationsPerSegment(10)
//                .withMaxCharsPerSegment(100);

//        composer.add(component.withSegmentationStrategy(pStrategy));

        // Abstract
        List<String> sAbstractList = new ArrayList<>();
        sAbstractList.add("http://geltlin.hucompute.org:8107");

//        composer.add(new DUUIRemoteDriver.Component(sAbstractList)
//                        .withParameter("model_name", "Google T5-base")
//                        .withParameter("summary_length", "75"))
//                .withWorkers(1);

        // CoRef
        List<String> sCoRefList = new ArrayList<>();
        sCoRefList.add("http://geltlin.hucompute.org:8106");
        composer.add(new DUUIRemoteDriver
                .Component(sCoRefList)
                .withSegmentationStrategy(pStrategy).withIgnoring200Error(true)).withWorkers(1);


        AsyncCollectionReader benchmarkReader = new AsyncCollectionReader("/storage/xmi/GerParCorDownload/Berlin", "xmi.gz", 1, 500, false, "/tmp/samplesegmentation");


        CollectionReaderDescription reader = CollectionReaderFactory.createReaderDescription(XmiReader.class
                , XmiReader.PARAM_LANGUAGE, "de"
                , XmiReader.PARAM_SOURCE_LOCATION, "/storage/xmi/GerParCorDownload/Berlin/xmi/**/*.xmi.gz"
        );

        long startTime = System.nanoTime();
//        composer.run(tCas);
//        composer.run(reader);
        composer.run(benchmarkReader, "segment");
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;  // divide by 1000000 to get milliseconds.
        System.out.println("-> " + duration + "ms");

        composer.shutdown();
    }
}
