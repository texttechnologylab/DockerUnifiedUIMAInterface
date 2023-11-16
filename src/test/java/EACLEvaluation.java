import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotation;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class EACLEvaluation {

    private static String KUBECONFIGPATH = "/etc/kubernetes/admin.conf";

    public static void caseOne(int iThreads, String runLabel, String sOutput, List<String> sLabels) throws Exception {

        String sInputPath = "/opt/gerparcor/gerparcor_sample1000_RANDOM";  // strg+L am entsprechenden Ort
        String sOutputPath = sOutput + "/" + runLabel + "_" + iThreads;
        String sSuffix = "xmi.gz";

        // Asynchroner reader für die Input-Dateien
        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, false);
        new File(sOutputPath).mkdir();

        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(iThreads);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmarkKubernetes.db").withConnectionPoolSize(iWorkers);

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


        DUUIKubernetesDriver kubernetes_driver = new DUUIKubernetesDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(kubernetes_driver, uima_driver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.

        // "docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4"
        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers)
                .withLabels(sLabels)
                .build().withSegmentationStrategy(pStrategy));

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        Runnable r = () -> {
            try {
                Thread.sleep(60000l); // wait 1 minute
                getKubeInfos(runLabel + "_" + iThreads, new File(sOutput));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        Thread pThread = new Thread(r);
        pThread.start();

        composer.run(pCorpusReader, runLabel + "_" + iThreads);

    }

    public static void caseTwo(int iThreads, String runLabel, String sOutput, List<String> sLabels) throws Exception {

//        String sInputPath = "/opt/gerparcor/gerparcor_sample1000_RANDOM";  // strg+L am entsprechenden Ort
        String sInputPath = "/home/staff_homes/abrami/Downloads/GerParCor_Test/";  // strg+L am entsprechenden Ort
        String sOutputPath = sOutput + "/" + runLabel + "_" + iThreads;
        String sSuffix = "xmi";

        // Asynchroner reader für die Input-Dateien
        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, false);
        new File(sOutputPath).mkdir();

        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(iThreads);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmarkKubernetes.db").withConnectionPoolSize(iWorkers);

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        sqlite.addNewRun(runLabel + "_" + iThreads, composer);


        DUUIKubernetesDriver kubernetes_driver = new DUUIKubernetesDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(kubernetes_driver, uima_driver, remoteDriver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByAnnotation()
                .withSegmentationClass(Sentence.class)
                .withMaxAnnotationsPerSegment(1000)
                .withMaxCharsPerSegment(100000);

        // "docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4"
//        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
//                .withScale(iWorkers)
//                .withLabels(sLabels.get(0))
//                .build().withSegmentationStrategy(pStrategy));
//
//        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:0.1")
//                .withScale(iWorkers)
//                .withLabels(sLabels.get(1))
//                .build().withSegmentationStrategy(pStrategy));

//        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-topic:0.0.3-cuda")
//                .withParameter("model_name", "chkla/parlbert-topic-german")
//                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
//                .withScale(iWorkers)
//                .withLabels(sLabels.get(1))
//                .build().withSegmentationStrategy(pStrategy));

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9714")
                .withParameter("model_name", "chkla/parlbert-topic-german")
                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
                .withScale(iWorkers)
                .build().withSegmentationStrategy(pStrategy));

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        Runnable r = () -> {
            try {
                Thread.sleep(60000l); // wait 1 minute
                getKubeInfos(runLabel + "_" + iThreads, new File(sOutput));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        Thread pThread = new Thread(r);
        pThread.start();

        composer.run(pCorpusReader, runLabel + "_" + iThreads);

    }

    public static void caseThree(int iThreads, String runLabel, String sOutput, List<String> sLabels) throws Exception {

        String sInputPath = "/opt/gerparcor/gerparcor_sample1000_RANDOM";  // strg+L am entsprechenden Ort
        String sOutputPath = sOutput + "/" + runLabel + "_" + iThreads;
        String sSuffix = "xmi.gz";

        // Asynchroner reader für die Input-Dateien
        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, false);
        new File(sOutputPath).mkdir();

        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(iThreads);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmarkKubernetes.db").withConnectionPoolSize(iWorkers);

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        sqlite.addNewRun(runLabel + "_" + iThreads, composer);


        DUUIKubernetesDriver kubernetes_driver = new DUUIKubernetesDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(kubernetes_driver, uima_driver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.


        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);

        // "docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4"
        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers)
                .withLabels(sLabels.get(0))
                .build().withSegmentationStrategy(pStrategy));

        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:0.1")
                .withScale(iWorkers)
                .withLabels(sLabels.get(1))
                .build().withSegmentationStrategy(pStrategy));


        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-topic:0.0.3-cuda")
                .withParameter("model_name", "chkla/parlbert-topic-german")
                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
                .withScale(iWorkers)
                .withLabels(sLabels.get(2))
                .build().withSegmentationStrategy(pStrategy));

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        Runnable r = () -> {
            try {
                Thread.sleep(60000l); // wait 1 minute
                getKubeInfos(runLabel + "_" + iThreads, new File(sOutput));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        Thread pThread = new Thread(r);
        pThread.start();

        composer.run(pCorpusReader, runLabel + "_" + iThreads);

    }

    public static void getKubeInfos(String sProcessName, File oPath) throws IOException {

        List<ProcessBuilder> processBuilders = new ArrayList<>(0);

        StringBuilder sb = new StringBuilder();

//        processBuilders.add(new ProcessBuilder("export", "KUBECONFIG=/etc/kubernetes/admin.conf"));
        processBuilders.add(new ProcessBuilder("kubectl", "get", "pods", "-o", "wide"));
        processBuilders.add(new ProcessBuilder("kubectl", "get", "deployments", "-o", "wide"));

        for (ProcessBuilder processBuilder : processBuilders) {
            Process p = null;
            try {

                Map<String, String> env = processBuilder.environment();
                env.put("KUBECONFIG", KUBECONFIGPATH);

                p = processBuilder.start();

                try {
                    // Create a new reader from the InputStream
                    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader br2 = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                    // Take in the input
                    String input;
                    while ((input = br.readLine()) != null) {
                        // Print the input
                        if (sb.length() > 0) {
                            sb.append("\n");
                        }
                        sb.append(input);
                    }
                    while ((input = br2.readLine()) != null) {
                        // Print the input
                        System.err.println(input);
                    }
                } catch (IOException io) {
                    io.printStackTrace();
                }

                p.waitFor();
            } catch (IOException | InterruptedException ex) {
                ex.printStackTrace();
            }

        }

        if (sb.length() > 0) {
            FileUtils.writeContent(sb.toString(), new File(oPath.getPath() + "/" + sProcessName + ".log"));
        }

        //docker run --gpus all nvidia/cuda:11.0-base nvidia-smi
    }

    @Test
    public void test() throws Exception {

        String sGlobalOutput = "/tmp/evaluation/";
        new File(sGlobalOutput).mkdir();

        Set<String> runs = new HashSet<>(0);

//        runs.add("2;caseOne;One;default=true");
//        runs.add("4;caseOne;One;default=true");
//        runs.add("8;caseOne;One;default=true");
//        runs.add("16;caseOne;One;default=true");

        runs.add("2;caseTwo;Two;default=true,gpu=all");
        runs.add("4;caseTwo;Two;default=true,gpu=all");

//        runs.add("2;caseThree;Three_1;default=true,gpu=all,gpu=all");
//        runs.add("4;caseThree;Three_1;default=true,gpu=all,gpu=all");
//
//        runs.add("2;caseThree;Three_2;default=true,gpu=1,gpu=2");
//        runs.add("4;caseThree;Three_2;default=true,gpu=1,gpu=2");

        for (String r : runs) {

            String[] sSplit = r.split(";");

            switch (sSplit[1]) {
                case "caseOne":
                    caseOne(Integer.valueOf(sSplit[0]), sSplit[2], sGlobalOutput, Arrays.stream(sSplit[3].split(",")).collect(Collectors.toList()));
                    break;

                case "caseTwo":
                    caseTwo(Integer.valueOf(sSplit[0]), sSplit[2], sGlobalOutput, Arrays.stream(sSplit[3].split(",")).collect(Collectors.toList()));
                    break;

                case "caseThree":
                    caseThree(Integer.valueOf(sSplit[0]), sSplit[2], sGlobalOutput, Arrays.stream(sSplit[3].split(",")).collect(Collectors.toList()));
                    break;

            }

        }

    }

}
