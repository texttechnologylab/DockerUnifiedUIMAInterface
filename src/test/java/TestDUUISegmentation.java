import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.dkpro.core.io.xmi.XmiReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotation;

public class TestDUUISegmentation {
    static public void main(String[] args) throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        composer.addDriver(dockerDriver);
        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.6").withImageFetching();

        /*component.withSegmentationStrategy(
                new DUUISegmentationStrategyByAnnotation()
                        .withSegmentationClass(Sentence.class)
                        .withMaxAnnotationsPerSegment(1000)
                        .withMaxCharsPerSegment(1000000)
        );*/

        composer.add(component);

        CollectionReaderDescription reader = CollectionReaderFactory.createReaderDescription(XmiReader.class
                , XmiReader.PARAM_LANGUAGE, "de"
                , XmiReader.PARAM_SOURCE_LOCATION, "gerparcor_test/12_06*.xmi.gz"
        );

        long startTime = System.nanoTime();
        composer.run(reader);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;  // divide by 1000000 to get milliseconds.
        System.out.println("-> " + duration + "ms");

        composer.shutdown();
    }
}
