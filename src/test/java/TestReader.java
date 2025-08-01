import org.apache.uima.UIMAException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReaderLazy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestReader {

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
        DUUISwarmDriver swarmDriver = new DUUISwarmDriver().withHostname("localhost");
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIPodmanDriver podmanDriver = new DUUIPodmanDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        pComposer.addDriver(uima_driver, remoteDriver, dockerDriver, podmanDriver);

    }

    @Test
    public void testReader() throws Exception {

        Set<DUUICollectionReader> readers = new HashSet<>(0);

        readers.add(new DUUIFileReader("/storage/xmi/GerParCorDownload/Germany/Historical/National/Weimar_Republic/1. Wahlperiode 1920/", ".xmi.gz", 1));

        pComposer.run(new DUUIAsynchronousProcessor(readers), "test");

    }

    @Test
    public void testReadability() throws Exception {

        DUUICollectionReader pReader = new DUUIFileReaderLazy("/storage/projects/CORE/projects2/Aclosed/single_xmis/uce_export_2025_06_30/raw/27aefa8f-a3dc-43c0-a9ab-87c64ccc4604", "html.gz.xmi.gz", 1);

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(pReader);

//        pComposer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-textreadability-diversity:0.1.1")
//                        .withParameter("diversity.homogenization", "False")
//                .withScale(1).build()
//                .withTimeout(10000)
//        );

        pComposer.add(new DUUIRemoteDriver.Component("http://localhost:8085").withScale(1)
                .withParameter("diversity.homogenization", "False")
                .build()
                .withTimeout(10000)
        );

        pComposer.add(new DUUISwarmDriver.Component("http://localhost:8085").withScale(1)

                .withParameter("diversity.homogenization", "False")
                .build()

                .withTimeout(10000)
        );

        pComposer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/readability/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, false,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        pComposer.run(processor, "test");

    }

}
