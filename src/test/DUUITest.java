import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordParser;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordPosTagger;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;
import org.junit.jupiter.api.Test;
import org.texttechnology.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnology.DockerUnifiedUIMAInterface.DUUIUIMADriver;
import org.xml.sax.SAXException;
import textimager.uima.io.abby.utility.XMIWriter;

import java.io.IOException;

import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

public class DUUITest {

    @Test
    public void XMIWriterTest() throws ResourceInitializationException, IOException, SAXException {

        int iWorkers = 8;

        DUUIComposer composer = new DUUIComposer().withWorkers(iWorkers);

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

        composer.addDriver(uima_driver);

        // UIMA Driver handles all native UIMA Analysis Engine Descriptions
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(StanfordPosTagger.class)
        ).withScale(iWorkers), DUUIUIMADriver.class);
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(StanfordParser.class)
        ).withScale(iWorkers), DUUIUIMADriver.class);
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(StanfordNamedEntityRecognizer.class)
        ).withScale(iWorkers), DUUIUIMADriver.class);

        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, "/tmp/output/",
                        XmiWriter.PARAM_PRETTY_PRINT, true,
                        XmiWriter.PARAM_OVERWRITE, true,
                        XmiWriter.PARAM_VERSION, "1.1",
                        XmiWriter.PARAM_COMPRESSION, "GZIP"
                        )
        ).withScale(iWorkers), DUUIUIMADriver.class);

        try {
            composer.run(createReaderDescription(XmiReaderModified.class,
                    XmiReader.PARAM_SOURCE_LOCATION, "/resources/public/abrami/Zobodat/xmi/txt/**.xmi.gz",
                    XmiWriter.PARAM_OVERWRITE, false
                    //XmiReader.PARAM_LANGUAGE, LanguageToolSegmenter.PARAM_LANGUAGE)
            ));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
