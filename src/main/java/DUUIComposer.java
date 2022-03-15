import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.base_cpm.CasDataCollectionReader;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Pipe;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

public class DUUIComposer {
    private Map<String, IDUUIDriverInterface> _drivers;
    private Vector<IDUUIPipelineComponent> _pipeline;

    private static final String DRIVER_OPTION_NAME = "duuid.composer.driver";

    public DUUIComposer() {
        _drivers = new HashMap<String, IDUUIDriverInterface>();
        _pipeline = new Vector<IDUUIPipelineComponent>();
    }

    public DUUIComposer addDriver(IDUUIDriverInterface driver) {
        _drivers.put(driver.getClass().getCanonicalName().toString(), driver);
        return this;
    }

    public <Y> DUUIComposer add(IDUUIPipelineComponent object, Class<Y> t) {
        object.setOption(DRIVER_OPTION_NAME, t.getCanonicalName().toString());
        IDUUIDriverInterface driver = _drivers.get(t.getCanonicalName().toString());
        if (driver == null) {
            throw new InvalidParameterException(format("No driver %s in the composer installed!", t.getCanonicalName().toString()));
        } else {
            if (!driver.canAccept(object)) {
                throw new InvalidParameterException(format("The driver %s cannot accept %s as input!", t.getCanonicalName().toString(), object.getClass().getCanonicalName().toString()));
            }
        }
        _pipeline.add(object);
        return this;
    }

    public static class PipelinePart {
        private IDUUIDriverInterface _driver;
        private String _uuid;

        PipelinePart(IDUUIDriverInterface driver, String uuid) {
            _driver = driver;
            _uuid = uuid;
        }

        public IDUUIDriverInterface getDriver() {
            return _driver;
        }

        public String getUUID() {
            return _uuid;
        }
    }

    public void run(CollectionReaderDescription reader) throws Exception {
        Exception catched = null;
        System.out.println("Instantiation the collection reader...");
        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);
        System.out.println("Instantiated the collection reader.");

        JCas jc = JCasFactory.createJCas();
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        try {
            instantiate_pipeline(idPipeline);

            while(collectionReader.hasNext()) {
                collectionReader.getNext(jc.getCas());
                run_pipeline(jc,idPipeline);
                jc.reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong, shutting down remaining components...");
            catched = e;
        }

        shutdown_pipeline(idPipeline);
        if (catched != null) {
            throw catched;
        }
    }

    private void instantiate_pipeline(Vector<PipelinePart> idPipeline) throws Exception {
        for (IDUUIPipelineComponent comp : _pipeline) {
            IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
            idPipeline.add(new PipelinePart(driver, driver.instantiate(comp)));
        }
        System.out.println("");
    }

    private DUUIEither run_pipeline(JCas jc, Vector<PipelinePart> pipeline) throws Exception {
        DUUIEither start = new DUUIEither(jc);

        for (PipelinePart comp : pipeline) {
            start = comp.getDriver().run(comp.getUUID(), start);
        }
        return start;
    }

    private void shutdown_pipeline(Vector<PipelinePart> pipeline) throws Exception {
        for (PipelinePart comp : pipeline) {
            System.out.printf("Shutting down %s...\n", comp.getUUID());
            comp.getDriver().destroy(comp.getUUID());
        }
        System.out.println("Shut down complete.\n");
    }

    public void run(JCas jc) throws Exception {
        Exception catched = null;
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        try {
            instantiate_pipeline(idPipeline);
            DUUIEither start = run_pipeline(jc,idPipeline);

            String cas = start.getAsString();
            System.out.printf("Result %s\n", cas);
            jc = start.getAsJCas();

            System.out.printf("Total number of transforms in pipeline %d\n", start.getTransformSteps());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong, shutting down remaining components...");
            catched = e;
        }
        shutdown_pipeline(idPipeline);
        if (catched != null) {
            throw catched;
        }
    }


    public static void main(String[] args) throws Exception {
        DUUIComposer composer = new DUUIComposer();
        DUUILocalDriver driver = new DUUILocalDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

        composer.addDriver(driver);
        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        composer.add(new DUUILocalDriver.Component("new:latest", true)
                        .withScale(2)
                        .withRunningAfterDestroy(false)
                , DUUILocalDriver.class);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9714"),
                DUUIRemoteDriver.class);

        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)
        ), DUUIUIMADriver.class);


        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        composer.run(jc);

        composer.run(createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, "test_corpora/**.txt",
                TextReader.PARAM_LANGUAGE, "en"));
    }
}