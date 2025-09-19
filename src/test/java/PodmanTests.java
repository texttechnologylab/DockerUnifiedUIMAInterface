import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.io.FileUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.xmi.XmiWriter;
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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

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
    public void spacy() throws Exception {
        pComposer.resetPipeline();

        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withImageFetching()
                .withScale(iWorkers)
                .build());

        pComposer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withImageFetching()
                .withScale(iWorkers)
                .build());

        pComposer.run(getCas(), "test");

    }

    @Test
    public void general() throws Exception {
// reset existing pipeline-components

        pComposer.resetPipeline();

        // laden eines Videos aus dem Ressourcen-Ordner
        ClassLoader classLoader = PodmanTests.class.getClassLoader();
        URL fVideo = classLoader.getResource("example.mp4");

        // convertieren eines Videos in einen Base64-String
        File fFile = new File(fVideo.getPath());
        byte[] bFile = FileUtils.readFileToByteArray(fFile);
        String encodedString = Base64.getEncoder().encodeToString(bFile);
        String pMimeType = Files.probeContentType(Path.of(fVideo.getPath()));

        JCas tCas = getCas();

        JCas videoCas = tCas.createView("video");
        videoCas.setSofaDataString(encodedString, pMimeType);
        videoCas.setDocumentLanguage("de");

        // erstellen einer weiteren View, für die Transcription
        JCas transcriptCas = tCas.createView("transcript");

        // Please note that the Docker images are first downloaded when they are called up for the first time.
        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/duui-whisperx:0.2")
//                .withGPU(true)
                .withImageFetching()
                .withSourceView("video")            // where is the video
                .withTargetView("transcript")
                .withScale(iWorkers)
                .build());

        // Please note that the Docker images are first downloaded when they are called up for the first time.
        pComposer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-whisperx:0.2")
//                .withGPU(true)
                .withImageFetching()
                .withSourceView("video")            // where is the video
                .withTargetView("transcript")
                .withScale(iWorkers)
                .build());


        pComposer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

//        // Please note that the Docker images are first downloaded when they are called up for the first time.
//        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
//                .withImageFetching()
//                .withScale(iWorkers)
//                .build());



        pComposer.run(tCas);
        pComposer.shutdown();

        JCasUtil.selectAll(tCas).stream().forEach(System.out::println);


    }

}
