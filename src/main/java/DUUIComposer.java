import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
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

    public void run(JCas jc) throws Exception {
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        Exception catched = null;
        try {
            for (IDUUIPipelineComponent comp : _pipeline) {
                IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
                idPipeline.add(new PipelinePart(driver, driver.instantiate(comp)));
            }
            System.out.println("");

            DUUIEither start = new DUUIEither(jc);

            for (PipelinePart comp : idPipeline) {
                start = comp.getDriver().run(comp.getUUID(), start);
            }

            String cas = start.getAsString();
            System.out.printf("Result %s\n", cas);
            jc = start.getAsJCas();

            System.out.printf("Total number of transforms in pipeline %d\n", start.getTransformSteps());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong, shutting down remaining components...");
            catched = e;
        }

        for (PipelinePart comp : idPipeline) {
            System.out.printf("Shutting down %s...\n", comp.getUUID());
            comp.getDriver().destroy(comp.getUUID());
        }
        System.out.println("Shut down complete.\n");
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
                AnalysisEngineFactory.createEngineDescription(OpenNlpSegmenter.class)
        ), DUUIUIMADriver.class);


        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        composer.run(jc);
    }
}