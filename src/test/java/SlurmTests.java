import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.DUUISlurmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class SlurmTests {
    @Test
    public void basicTest() throws Exception {
        int iWorkers = 1; //
        JCas jc = JCasFactory.createText("hello world");
        DocumentMetaData dmd = new DocumentMetaData(jc);
        dmd.setDocumentId("test05");
        dmd.setDocumentTitle("univers test05");
        dmd.addToIndexes();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUILuaSandbox sandbox = new DUUILuaSandbox();
        sandbox._allowAllJavaClasses = true;
        ctx.withSandbox(sandbox);
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        // driver hinzufügen
        DUUISlurmDriver slurmDriver = new DUUISlurmDriver();

        composer.addDriver(uimaDriver, slurmDriver);

        composer.resetPipeline();
        DUUIPipelineComponent com1 = new DUUISlurmDriver.Component(new DUUIPipelineComponent()
                .withSlurmCpus("2")
                .withSlurmGPU("0")
                .withSlurmMemory("1G")
                .withSlurmImagePort("9714")
                .withSlurmJobName("test1")
                .withSlurmEntryLocation("cd /usr/src/app")
                .withSlurmErrorLocation("/tmp")
                .withSlurmSaveIn("/home/jd/again.sif")
                .withSlurmRuntime("1:00:00")
                .withScale(1)
                .withSlurmUvicorn("uvicorn textimager_duui_spacy:app")
        ).build();


        composer.add(com1);

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
