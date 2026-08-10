package slurmDriver;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.DUUISlurmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmRestPhysical;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.ConfigManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.SlurmConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class BenchmarkTest {
    public static String pass = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjM5MzE2MjcxOTksImlhdCI6MTc4NDE0MzU1Mywic3VuIjoiamQifQ.iOH2wdmZB8ssbbFnHpIf-lYUtZ8gO4nfA5exlm3kbNM";
    public static String sInputPath = "/home/jd/eschar/GerParCor_Sample/GerParCor/Sample_mini";  //
    public static void caseOneSlurm(int iThreads, String runLabel, String sOutput, List<String> sLabels) throws Exception {
        String sOutputPath = sOutput + "/" + runLabel + "_" + iThreads;
        String sSuffix = "xmi.gz";

        // Asynchroner reader für die Input-Dateien
        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 1, 100, false, "/home/jd/eschar/results");
        new File(sOutputPath).mkdir();

        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(iThreads);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("/home/jd/benchmark_localtest.db").withConnectionPoolSize(iWorkers);

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        sqlite.addNewRun(runLabel + "_" + iThreads, composer);

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);
        SlurmConfig config = new SlurmConfig();
        SlurmRestPhysical sr = new SlurmRestPhysical(ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml")),pass);
        DUUISlurmDriver slurm_driver = new DUUISlurmDriver(sr);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(slurm_driver, uima_driver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.

        // "docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4"




        DUUIPipelineComponent com1 = new DUUISlurmDriver.Component(
                new DUUIPipelineComponent().
                        withSlurmPartition("normal").
                        withSlurmNodelist("jd-Inspiron-14-5410").
                        withSlurmWorkDir("/home/jd/CODE").
                        withSlurmUvicorn("uvicorn textimager_duui_spacy:app").
                        withSlurmSIFName("spacy").
                        withSlurmMemory("1024").
                        withSlurmRuntime("600").withSlurmCPUs("1").
                        withSlurmSaveIn("/home/jd/CODE/spacy.sif").withSlurmJobName("spacy").
                        withSlurmGPU("0").withScale(iThreads).withSegmentationStrategy(pStrategy)
        ).build();

        composer.add(com1);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, runLabel + "_" + iThreads);

    }


    // select sum(durationAnnotator) as su from pipeline_document_perf where pipelinename = 'Three_1_2';
    @Test
    public void test() throws Exception {

        String sGlobalOutput = "/home/jd/eschar/global_results/";
        new File(sGlobalOutput).mkdir();

        List<String> runs = new ArrayList<>(0);
        runs.add("1;caseOneSlurm;OneSlurm_1;default=true");

        for (String r : runs) {

            String[] sSplit = r.split(";");

            switch (sSplit[1]) {

                case "caseOneSlurm":
                    caseOneSlurm(Integer.valueOf(sSplit[0]), sSplit[2], sGlobalOutput, Arrays.stream(sSplit[3].split(",")).collect(Collectors.toList()));
                    break;


            }

        }

    }



    @Test
    public void noRestOnlyBatchWithPortForwarding(){



    }

}







