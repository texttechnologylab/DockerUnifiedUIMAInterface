import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.resources.CompressionMethod;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIReaderComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDynamicReaderLazy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

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
    public void DynamicReaderInitTest() throws Exception {
        DUUIPipelineComponent readerComp = new DUUIRemoteDriver.Component("http://0.0.0.0:9714").build();
        List<DUUIPipelineComponent> compList = List.of(readerComp);
        Path filePath = Path.of("/home/staff_homes/lehammer/Documents/work/AnnisReader/data/test_data/DDD-AD-Genesis.zip");
        DUUIDynamicReaderLazy dynamicReader = new DUUIDynamicReaderLazy(filePath, compList);
        // TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void DynamicReaderInitTest2() throws Exception {
        Path filePath = Path.of("/home/staff_homes/lehammer/Documents/work/AnnisReader/data/test_data/DDD-AD-Genesis.zip");
        DUUIPipelineComponent readerComp = new DUUIRemoteDriver.Component("http://0.0.0.0:9714").build();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIReaderComposer composer = new DUUIReaderComposer()
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
        composer.add(readerComp);
        composer.instantiate_pipeline();
        System.out.println(composer.get_isServiceStarted());
        composer.instantiateReaderPipeline(filePath);
        System.out.println("go");

        JCas _basic1 = JCasFactory.createJCas();
        composer.run(_basic1);
        System.out.println(_basic1.getSofa());
        System.out.println("---------------------------------------");

        JCas _basic2 = JCasFactory.createJCas();
        composer.run(_basic2);
        System.out.println(_basic2.getSofa());
        System.out.println("---------------------------------------");

        JCas _basic3 = JCasFactory.createJCas();
        composer.run(_basic3);
        System.out.println(_basic3.getSofa());
        System.out.println("---------------------------------------");

        JCas _basic4 = JCasFactory.createJCas();
        composer.run(_basic4);
        System.out.println(_basic4.getSofa());
        System.out.println("---------------------------------------");
    }

    @Test
    public void DynamicReaderTest() throws Exception {
        //DUUIPipelineComponent readerComp = new DUUIRemoteDriver.Component("http://0.0.0.0:9714").build();


        DUUIPipelineComponent readerComp = new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-annis_reader_unfinished:0.1")
                .withScale(iWorker).withImageFetching()
                .build().withTimeout(30);


        List<DUUIPipelineComponent> compList = List.of(readerComp);

        ClassLoader classLoader = ReaderCompTest.class.getClassLoader();
        URL fAnnis = classLoader.getResource("reading/DDD-AD-Benediktiner_Regel.zip");

        Path filePath = Path.of(fAnnis.getPath());
        DUUIDynamicReaderLazy dynamicReader = new DUUIDynamicReaderLazy(filePath, compList);


        String sOutputPath = "/tmp/annis/";
        // Asynchroner reader für die Input-Dateien
        DUUIAsynchronousProcessor pProcessor = new DUUIAsynchronousProcessor(dynamicReader);
        new File(sOutputPath).mkdir();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorker);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(docker_driver, uima_driver
                ,swarm_driver, remote_driver
        );

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, CompressionMethod.BZIP2
        )).withScale(iWorker).build());

        composer.run(pProcessor, "DynamicReaderTest");

    }


}
