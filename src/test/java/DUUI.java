import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.BeforeAll;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class DUUI {

    private static final int iWorkers = 1;
    private static DUUIComposer pComposer = null;

    @BeforeAll
    public static void init() throws IOException, URISyntaxException, UIMAException, SAXException {

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        pComposer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();

        pComposer.addDriver(uima_driver, remoteDriver, dockerDriver);


    }

    /**
     * Initialization of a sample CAS document
     *
     * @return
     * @throws ResourceInitializationException
     * @throws CASException
     */
    public static JCas getCas() throws ResourceInitializationException, CASException {
        // init a CAS with a static text.
        JCas pCas = JCasFactory.createText("Java, indonesisch Jawa (nach alter Schreibweise Djawa; Aussprache: [dʒawa], im Deutschen zumeist [ˈjaːva]) ist neben Sumatra, Borneo und Sulawesi eine der vier Großen Sundainseln. Die Insel gehört vollständig zur Republik Indonesien, auf ihr liegt auch die größte Stadt und ehemalige Hauptstadt Indonesiens, Jakarta.", "de");

        // Define some metadata to serialize the CAS with the xmi writer
        DocumentMetaData dmd = new DocumentMetaData(pCas);
        dmd.setDocumentId("Java (Insel)");
        dmd.setDocumentTitle("Java (Insel)");
        dmd.addToIndexes();

        return pCas;
    }

    public void testDockerSpacy() throws URISyntaxException, IOException, CompressorException, InvalidXMLException, SAXException, ResourceInitializationException {

        pComposer.resetPipeline();

        pComposer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withImageFetching()
                .withScale(iWorkers)
                .build());

        pComposer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, System.getProperty("java. io. tmpdir"),
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

    }

}
