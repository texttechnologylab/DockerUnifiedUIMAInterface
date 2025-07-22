package slurmDriver;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.DUUISlurmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.slurmInDocker.SlurmRest;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.*;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class universTest {
    @Test
    public void universTest() throws Exception {
        int iWorkers = 2; // define the number of workers

        JCas jc = JCasFactory.createJCas(); // An empty CAS document is defined.

// load content into jc ...

// Defining LUA-Context for communication
        DUUILuaContext ctx = LuaConsts.getJSON();
// The composer is defined and initialized with a standard Lua context as well with a storage backend.
        DUUIComposer composer = new DUUIComposer().withLuaContext(ctx).withSkipVerification(true);

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

// A driver must be added before components can be added for it in the composer. After that the composer is able to use the individual drivers.
        composer.addDriver(uima_driver);
// Adding a UIMA annotator for writing the result of the pipeline as XMI files.
        composer.add(new DUUIUIMADriver.Component(
                createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, "/tmp/test"
                )).withScale(iWorkers));

// The document is processed through the pipeline. In addition, files of entire repositories can be processed.
        composer.run(jc);
    }


    @Test
    public void universTest2() throws Exception {

        int iWorkers = 5; // define the number of workers

        JCas jc = JCasFactory.createText("hello world");
        DocumentMetaData dmd = new DocumentMetaData(jc);
        dmd.setDocumentId("test01");
        dmd.setDocumentTitle("univers test2");
        dmd.addToIndexes();


        //


        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUILuaSandbox sandbox = new DUUILuaSandbox();
        sandbox._allowAllJavaClasses = true;
        ctx.withSandbox(sandbox);
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver duuidDriver = new DUUIDockerDriver();
        composer.addDriver(uimaDriver, duuidDriver);
        // reset also init make sure before run empty
        composer.resetPipeline();


        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withImageFetching()
                .withScale(iWorkers)
                .build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/nlp/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());

        composer.run(jc);
    }


}
