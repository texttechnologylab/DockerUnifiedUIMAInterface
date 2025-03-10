import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.resources.CompressionMethod;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDynamicReaderLazy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotationFast;
import org.texttechnologylab.annotation.type.Image;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;



public class ReaderCompTest {
    int iWorker = 1;

    public ReaderCompTest() throws URISyntaxException, IOException {
    }

    @Test
    public void ANNISReaderCompAsCompTest() throws Exception {
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorker);
        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(docker_driver, uima_driver
                ,swarm_driver, remote_driver);


        composer.add(new DUUIRemoteDriver.Component("http://0.0.0.0:9714").build());
        JCas empt_jcas = JCasFactory.createJCas();
        composer.run(empt_jcas);

        System.out.println(empt_jcas.getSofa());
        JCasUtil.select(empt_jcas, Token.class).stream()
                .map(Token::getCoveredText)
                .forEach(System.out::println);
    }

    @Test
    public void ANNISReaderCompAsCompInit() throws Exception {
        DUUIPipelineComponent readerComp = new DUUIRemoteDriver.Component("http://0.0.0.0:9714").build();
        Path filePath = Path.of("/home/staff_homes/lehammer/Documents/work/AnnisReader/data/test_data/DDD-AD-Genesis.zip");
        Integer nDocs = DUUIDynamicReaderLazy.initComponent(readerComp, filePath);
        System.out.println(nDocs);

    }
}
