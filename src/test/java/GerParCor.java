import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIGerParCorReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer.GerParCorWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class GerParCor {

    public static void runTest(int iScale, String sName, String sImage, String sInput, String sOutput, DUUISqliteStorageBackend sqlite) throws Exception {

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());


        composer.withStorageBackend(sqlite);
        sqlite.addNewRun(sName, composer);

        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(dockerDriver, remoteDriver, uimaDriver);

        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component(sImage).withScale(iScale).withImageFetching();

//        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByAnnotation()
//                .withSegmentationClass(Sentence.class)
//                .withMaxCharsPerSegment(1000000)
//                .withMaxAnnotationsPerSegment(10);
        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);

//        composer.add(componentSentence);
        DUUIUIMADriver.Component language = new DUUIUIMADriver.Component(createEngineDescription(SetLanguage.class, SetLanguage.PARAM_LANGUAGE, "de")).withScale(iScale);

        DUUIUIMADriver.Component removeOverlappingSentences = new DUUIUIMADriver.Component(
                createEngineDescription(RemoveOverlappingAnnotations.class,
                        RemoveOverlappingAnnotations.PARAM_TYPE_LIST, Sentence.class.getName())
        ).withScale(iScale);

        composer.add(language);

//        composer.add(component);
        composer.add(component.withSegmentationStrategy(pStrategy));
        composer.add(removeOverlappingSentences);

        AnalysisEngineDescription writerEngine = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutput + "/" + sName,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        AsyncCollectionReader dataReader = new AsyncCollectionReader("/home/gabrami/Downloads/GerParCorTest/sentence", "xmi.gz", 1, true);
        AsyncCollectionReader dataReader = new AsyncCollectionReader(sInput, "xmi.gz", 1, true, sOutput + "/" + sName);

        composer.run(dataReader, sName);

        composer.shutdown();


    }


    public static void runTestGPU(int iScale, String sName, String sURL, String sInput, String sOutput, DUUISqliteStorageBackend sqlite) throws Exception {

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.withStorageBackend(sqlite);
        sqlite.addNewRun(sName, composer);

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver);

        DUUIUIMADriver.Component language = new DUUIUIMADriver.Component(createEngineDescription(SetLanguage.class, SetLanguage.PARAM_LANGUAGE, "de")).withScale(iScale);
        DUUIRemoteDriver.Component component = new DUUIRemoteDriver.Component(sURL).withScale(iScale);

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);

//        composer.add(componentSentence);

        DUUIUIMADriver.Component removeOverlappingSentences = new DUUIUIMADriver.Component(
                createEngineDescription(RemoveOverlappingAnnotations.class,
                        RemoveOverlappingAnnotations.PARAM_TYPE_LIST, Sentence.class.getName())
        ).withScale(iScale);

        composer.add(language);
//        composer.add(component);
        composer.add(component.withSegmentationStrategy(pStrategy));
        composer.add(removeOverlappingSentences);

        AnalysisEngineDescription writerEngine = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutput + "/" + sName,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        AsyncCollectionReader dataReader = new AsyncCollectionReader("/home/gabrami/Downloads/GerParCorTest/sentence", "xmi.gz", 1, true);
        AsyncCollectionReader dataReader = new AsyncCollectionReader(sInput, "xmi.gz", 1, true, sOutput + "/" + sName);

        composer.run(dataReader, sName);

        composer.shutdown();


    }


    public static void startGPUContainer(String sImage, String sName, int iPort) throws IOException {

        ProcessBuilder pProcess = new ProcessBuilder("docker", "run", "-d", "--rm", "--gpus", "all", "-p", "" + iPort + ":9714", "--name", sName, sImage);
        pProcess.directory(new File("/tmp/"));

        Process p = null;
        try {
            p = pProcess.start();

            try {
                // Create a new reader from the InputStream
                BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader br2 = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                // Take in the input
                String input;
                while ((input = br.readLine()) != null) {
                    // Print the input
                    System.out.println(input);
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

        //docker run --gpus all nvidia/cuda:11.0-base nvidia-smi
    }

    public static void stopContainer(String sName) throws IOException {

        ProcessBuilder pProcess = new ProcessBuilder("docker", "stop", sName);

        Process p = null;
        try {
            p = pProcess.start();

            try {
                // Create a new reader from the InputStream
                BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader br2 = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                // Take in the input
                String input;
                while ((input = br.readLine()) != null) {
                    // Print the input
                    System.out.println(input);
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

    @Test
    public void process() throws Exception {

        int iScale = 1;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("gerparcorReloaded.db")
                .withConnectionPoolSize(1);
        composer.withStorageBackend(sqlite);

        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(dockerDriver, remoteDriver, uimaDriver);

//        DUUIDockerDriver.Component componentSentence = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-sentencizer:0.2.1").withScale(iScale).withImageFetching();
        DUUIUIMADriver.Component componentSentence = new DUUIUIMADriver.Component(
                createEngineDescription(SimpleSegmenter.class,
                        SimpleSegmenter.PARAM_SENTENCE_LENGTH, "50000")
        ).withScale(iScale);

        DUUIUIMADriver.Component pseudoremover = new DUUIUIMADriver.Component(
                createEngineDescription(AnnotationCommentsRemover.class,
                        AnnotationCommentsRemover.PARAM_ANNOTATION_KEY, "PSEUDOSENTENCE")
        ).withScale(iScale);

//        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-sentencizer-stanza:0.0.1").withScale(iScale).withImageFetching();
        DUUIRemoteDriver.Component component = new DUUIRemoteDriver.Component("http://localhost:9999").withScale(iScale);
//        DUUIDockerDriver.Component component = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4").withScale(iScale).withImageFetching();

//        DUUISegmentationStrategy pStrategy = null;
        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(800000)
                .withOverlap(500);

//        composer.add(componentSentence);

        DUUIUIMADriver.Component removeOverlappingSentences = new DUUIUIMADriver.Component(
                createEngineDescription(RemoveOverlappingAnnotations.class,
                        RemoveOverlappingAnnotations.PARAM_TYPE_LIST, Sentence.class.getName())
        ).withScale(iScale);

        composer.add(component.withSegmentationStrategy(pStrategy));
        composer.add(removeOverlappingSentences);


//        composer.add(pseudoremover);

//        DUUIDockerDriver.Component sentimentComponent = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.2").withScale(iScale)
//                .withParameter("model_name", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
//                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence");
//
//        composer.add(sentimentComponent.withSegmentationStrategy(pStrategy));

//        DUUIRemoteDriver.Component topicComponent = new DUUIRemoteDriver.Component("http://warogast.hucompute.org:9000", "http://warogast.hucompute.org:9001").withScale(2)
//                .withParameter("model_name", "chkla/parlbert-topic-german")
//                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence");
//
//        composer.add(topicComponent.withSegmentationStrategy(pStrategy));
//
//        DUUIRemoteDriver.Component srlComponent = new DUUIRemoteDriver.Component("http://141.2.108.253:9000", "http://141.2.108.253:9001").withScale(2);
//
//        composer.add(srlComponent.withSegmentationStrategy(pStrategy));

        AnalysisEngineDescription writerEngine = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/GerParCor/xmi",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        AsyncCollectionReader dataReader = new AsyncCollectionReader("/home/gabrami/Downloads/GerParCorTest/sentence", "xmi.gz", 1, true);
        AsyncCollectionReader dataReader = new AsyncCollectionReader("/storage/projects/abrami/GerParCor/xmi", "xmi.gz", 1, true, "/tmp/GerParCor/xmi");

        composer.run(dataReader, "reloaded");

        composer.shutdown();

    }

    @Test
    public void testDanielCPU() throws Exception {

        Map<String, String> setContainer = new HashMap<>(0);
//        setContainer.put("Trankit", "docker.texttechnologylab.org/duui-sentencizer-trankit-cuda1:0.2");
        setContainer.put("Stanza", "docker.texttechnologylab.org/duui-sentencizer-stanza:0.0.1");
//        setContainer.put("CoreNLP", "docker.texttechnologylab.org/duui-sentencizer-corenlp:0.0.1");
//        setContainer.put("Segtok", "docker.texttechnologylab.org/duui-sentencizer-segtok:0.0.1");
//        setContainer.put("Syntok", "docker.texttechnologylab.org/duui-sentencizer-syntok:0.0.1");
//        setContainer.put("spacy-senter-lg", "docker.texttechnologylab.org/duui-sentencizer-spacy-senter-lg:0.0.1");
//        setContainer.put("spacy-senter-md", "docker.texttechnologylab.org/duui-sentencizer-spacy-senter-md:0.0.1");
//        setContainer.put("spacy-senter-sm", "docker.texttechnologylab.org/duui-sentencizer-spacy-senter-sm:0.0.1");
//        setContainer.put("spacy-senter-ruler", "docker.texttechnologylab.org/duui-sentencizer-spacy-ruler:0.0.1");
//        setContainer.put("spacy-senter-trf", "docker.texttechnologylab.org/duui-sentencizer-spacy-trf:0.0.1");
//        setContainer.put("spacy-parser-lg", "docker.texttechnologylab.org/duui-sentencizer-spacy-parser-lg:0.0.1");
//        setContainer.put("spacy-parser-md", "docker.texttechnologylab.org/duui-sentencizer-spacy-parser-md:0.0.1");
//        setContainer.put("spacy-parser-sm", "docker.texttechnologylab.org/duui-sentencizer-spacy-parser-sm:0.0.1");


//        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("testSegmenting.db")
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("testNewStanza.db")
                .withConnectionPoolSize(setContainer.size());

        int iScale = 1;
        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_out/isengart_workers1_gerparcor_sample1000_RANDOM_100_segmentedFalse_02_sentence";
//        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_sample1000_RANDOM_100";
//        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_sample1000_SMALLEST_100";
//	  String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/conll2003/xmi_empty";


        String sOuptut = "/tmp/duui/testNewStanza";

        setContainer.keySet().stream().forEach(k -> {
            String v = setContainer.get(k);
            try {
                runTest(iScale, k, v, sInput, sOuptut, sqlite);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

    @Test
    public void testDanielGPU() throws Exception {

        Map<String, String> setContainer = new HashMap<>(0);
        setContainer.put("Trankit", "docker.texttechnologylab.org/duui-sentencizer-trankit-cuda1:0.2");
        setContainer.put("Stanza", "docker.texttechnologylab.org/duui-sentencizer-stanza:0.0.1");
        setContainer.put("CoreNLP", "docker.texttechnologylab.org/duui-sentencizer-corenlp:0.0.1");
        setContainer.put("Segtok", "docker.texttechnologylab.org/duui-sentencizer-segtok:0.0.1");
        setContainer.put("Syntok", "docker.texttechnologylab.org/duui-sentencizer-syntok:0.0.1");
        setContainer.put("spacy-senter-lg", "docker.texttechnologylab.org/duui-sentencizer-spacy-senter-lg:0.0.1");
        setContainer.put("spacy-senter-md", "docker.texttechnologylab.org/duui-sentencizer-spacy-senter-md:0.0.1");
        setContainer.put("spacy-senter-sm", "docker.texttechnologylab.org/duui-sentencizer-spacy-senter-sm:0.0.1");
        setContainer.put("spacy-senter-ruler", "docker.texttechnologylab.org/duui-sentencizer-spacy-ruler:0.0.1");
        setContainer.put("spacy-senter-trf", "docker.texttechnologylab.org/duui-sentencizer-spacy-trf:0.0.1");
        setContainer.put("spacy-parser-lg", "docker.texttechnologylab.org/duui-sentencizer-spacy-parser-lg:0.0.1");
        setContainer.put("spacy-parser-md", "docker.texttechnologylab.org/duui-sentencizer-spacy-parser-md:0.0.1");
        setContainer.put("spacy-parser-sm", "docker.texttechnologylab.org/duui-sentencizer-spacy-parser-sm:0.0.1");


        //.withConnectionPoolSize(setContainer.size());
//        DUUISqliteStorageBackend sqliteGPU = new DUUISqliteStorageBackend("testSegmenting_GPU.db")
//                .withConnectionPoolSize(setContainer.size());

        int iScale = 1;
//        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_sample1000_SMALLEST_100";
//        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_sample1000_RANDOM_100";

        DUUISqliteStorageBackend sqliteGPU = new DUUISqliteStorageBackend("testSegmenting_conll2023_GPU_seg.db")
                .withConnectionPoolSize(setContainer.size());

//        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_sample1000_RANDOM_100";
//        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/gerparcor_sample1000_SMALLEST_100";
        String sInput = "/storage/projects/baumartz/duui_segmentation_data/paper_data/samples/conll2003/xmi_empty";


        String sOuptut = "/tmp/duui/sentenceTest_conll2023_seg";


        setContainer.keySet().stream().forEach(k -> {
            String v = setContainer.get(k);
            try {
                startGPUContainer(v, k, 9999);
                runTestGPU(iScale, k + "_GPU", "http://localhost:9999", sInput, sOuptut + "/GPU", sqliteGPU);
                stopContainer(k);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    stopContainer(k);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

    }

    @Test
    public void renameGerParCor() throws Exception {

        int iScale = 4;

        String sOutput = "/storage/xmi/GerParCorDownload/Germany/National/Bundestag/";

//        String sInput = "/tmp/Bundestag/15";
        String sInput = "/storage/xmi/GerParCorDownload/Germany/National/";

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);

        DUUIUIMADriver.Component component = new DUUIUIMADriver.Component(
                createEngineDescription(RenameMetaDataGerParCor.class,
                        RenameMetaDataGerParCor.PARAM_OUTPUT, sOutput
                )
        ).withScale(iScale);

        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);

        composer.add(spacy.withSegmentationStrategy(pStrategy));

        composer.add(component);

        AnalysisEngineDescription writerEngine = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutput,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        AsyncCollectionReader dataReader = new AsyncCollectionReader("/home/gabrami/Downloads/GerParCorTest/sentence", "xmi.gz", 1, true);
        AsyncCollectionReader dataReader = new AsyncCollectionReader(sInput, "xmi", 1, false, sOutput);

        composer.run(dataReader, "rename");

        composer.shutdown();

    }

    @Test
    public void DBTest() throws Exception {

        int iScale = 5;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);


        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(spacy.withSegmentationStrategy(pStrategy));

//        composer.add(component);


        AnalysisEngineDescription writerEngine = createEngineDescription(GerParCorWriter.class,
                GerParCorWriter.PARAM_DBConnection, "/home/staff_homes/abrami/Projects/GitHub/abrami/DockerUnifiedUIMAInterface/src/main/resources/rw"
        );
        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        CollectionReaderDescription reader = createReaderDescription(GerParCorReader.class,
//                GerParCorReader.PARAM_DBConnection, "/home/staff_homes/abrami/Projects/GitHub/abrami/DockerUnifiedUIMAInterface/src/main/resources/rw",
//                GerParCorReader.PARAM_Query, "{\"annotations.Token\": 0 }"
//        );

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/test",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngine).build());

        DUUICollectionReader gerparcorReader = new DUUIGerParCorReader(new MongoDBConfig("/home/staff_homes/abrami/Projects/GitHub/abrami/DockerUnifiedUIMAInterface/src/main/resources/rw"), "{\"annotations.Token\": 0 }");
        ((DUUIGerParCorReader) gerparcorReader).withOverrideMeta();
        DUUIAsynchronousProcessor asyncProcessor = new DUUIAsynchronousProcessor(gerparcorReader);

        composer.run(asyncProcessor, "test");


    }

    String sPathDB = "/resources/public/abrami/Evaluation/DockerUnifiedUIMAInterface_new/src/main/resources/rw";

    @Test
    public void GerParCorFullspaCy() throws Exception {

        int iScale = 1;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);

        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(spacy.withSegmentationStrategy(pStrategy));

//        composer.add(component);

        AnalysisEngineDescription writerEngine = createEngineDescription(GerParCorWriter.class,
                GerParCorWriter.PARAM_DBConnection, sPathDB,
                GerParCorWriter.PARAM_compress, "true"
        );
        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        CollectionReaderDescription reader = createReaderDescription(GerParCorReader.class,
//                GerParCorReader.PARAM_DBConnection, "/home/staff_homes/abrami/Projects/GitHub/abrami/DockerUnifiedUIMAInterface/src/main/resources/rw",
//                GerParCorReader.PARAM_Query, "{\"annotations.Token\": 0 }"
//        );

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/test",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngine).build());

        DUUICollectionReader gerparcorReader = new DUUIGerParCorReader(new MongoDBConfig(sPathDB), "{\"annotations.Token\": 0 }");
        ((DUUIGerParCorReader) gerparcorReader).withOverrideMeta();
        DUUIAsynchronousProcessor asyncProcessor = new DUUIAsynchronousProcessor(gerparcorReader);

        composer.run(asyncProcessor, "spacy");


    }

    @Test
    public void GerParCorFullSentiment() throws Exception {

        int iScale = 10;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);

        DUUIDockerDriver.Component sentiment = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.2").withScale(iScale)
                .withParameter("model_name", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence");

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(sentiment.withSegmentationStrategy(pStrategy));

//        composer.add(component);


        AnalysisEngineDescription writerEngine = createEngineDescription(GerParCorWriter.class,
                GerParCorWriter.PARAM_DBConnection, sPathDB
        );
        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        CollectionReaderDescription reader = createReaderDescription(GerParCorReader.class,
//                GerParCorReader.PARAM_DBConnection, "/home/staff_homes/abrami/Projects/GitHub/abrami/DockerUnifiedUIMAInterface/src/main/resources/rw",
//                GerParCorReader.PARAM_Query, "{\"annotations.Token\": 0 }"
//        );

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/test",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngine).build());

        DUUICollectionReader gerparcorReader = new DUUIGerParCorReader(new MongoDBConfig(sPathDB), "{\"annotations.Token\": 0 }");
        ((DUUIGerParCorReader) gerparcorReader).withOverrideMeta();
        DUUIAsynchronousProcessor asyncProcessor = new DUUIAsynchronousProcessor(gerparcorReader);

        composer.run(asyncProcessor, "sentiment");


    }

    @Test
    public void GerParCorUpdateMetaData() throws Exception {

        int iScale = 20;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver, uimaDriver, dockerDriver);

//        composer.add(component);

        AnalysisEngineDescription writerEngine = createEngineDescription(GerParCorWriter.class,
                GerParCorWriter.PARAM_DBConnection, sPathDB,
                GerParCorWriter.PARAM_compress, "true"
        );
        composer.add(new DUUIUIMADriver.Component(writerEngine).withScale(iScale).build());

//        CollectionReaderDescription reader = createReaderDescription(GerParCorReader.class,
//                GerParCorReader.PARAM_DBConnection, "/home/staff_homes/abrami/Projects/GitHub/abrami/DockerUnifiedUIMAInterface/src/main/resources/rw",
//                GerParCorReader.PARAM_Query, "{\"annotations.Token\": 0 }"
//        );

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/test",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngine).build());

        DUUICollectionReader gerparcorReader = new DUUIGerParCorReader(new MongoDBConfig(sPathDB), "{\"annotations.Token\": { $gt : 0 }  }");
        ((DUUIGerParCorReader) gerparcorReader).withOverrideMeta();
        DUUIAsynchronousProcessor asyncProcessor = new DUUIAsynchronousProcessor(gerparcorReader);

        composer.run(asyncProcessor, "upateMetadata");


    }

}
