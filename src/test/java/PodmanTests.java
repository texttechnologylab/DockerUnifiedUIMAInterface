import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;

@Nested
public class PodmanTests {

    // common class-attributes
    private static DUUIComposer pComposer = null;
    private static int iWorkers = 1;

    /**
     * Initialization of DUUI for each test, saves lines of code.
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws UIMAException
     * @throws SAXException
     */
    @BeforeAll
    public static void init() throws IOException, URISyntaxException, UIMAException, SAXException {

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        pComposer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIPodmanDriver podmanDriver = new DUUIPodmanDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        pComposer.addDriver(uima_driver, remoteDriver, dockerDriver, podmanDriver);

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
        JCas pCas = JCasFactory.createText("Ich finde dieses Programm läuft sehr gut. Ich überlege wie ich dieses für meine Bachelor-Arbeit nachnutzen kann.", "de");

        // Define some metadata to serialize the CAS with the xmi writer
        DocumentMetaData dmd = new DocumentMetaData(pCas);
        dmd.setDocumentId("test");
        dmd.setDocumentTitle("DUUI Test-Dokument");
        dmd.addToIndexes();

        return pCas;
    }

    @Test
    public void general() throws Exception {
// reset existing pipeline-components

        pComposer.resetPipeline();

        // Please note that the Docker images are first downloaded when they are called up for the first time.
        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withImageFetching()
                .withScale(iWorkers)
                .build());

        JCas tCas = getCas();

        pComposer.run(tCas);


    }

}
