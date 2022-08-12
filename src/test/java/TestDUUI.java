import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIPipelineAnnotationComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIPipelineDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIMockStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.annotation.type.Taxon;
import org.texttechnologylab.utilities.helper.FileUtils;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestDUUI {

    @Test
    public void creatingSample() throws IOException, UIMAException {

        String sInputPath = TestDUUI.class.getClassLoader().getResource("Bundestag.txt").getPath();
        String sOutputPath = "/tmp/sample_splitted/";
        new File(sOutputPath).mkdir();

        String sContent = FileUtils.getContentFromFile(new File(sInputPath));

        JCas pCas = JCasFactory.createText(sContent, "de");

        AggregateBuilder pipeline = new AggregateBuilder();

        pipeline.add(createEngineDescription(BreakIteratorSegmenter.class,
                BreakIteratorSegmenter.PARAM_WRITE_SENTENCE, true,
                BreakIteratorSegmenter.PARAM_LANGUAGE, "de",
                BreakIteratorSegmenter.PARAM_WRITE_TOKEN, false
        ));

        SimplePipeline.runPipeline(pCas, pipeline.createAggregateDescription());

        int iMaxLength = pCas.getDocumentText().length();
        int iMaxSplit = 10;

        int iSplitIterator = iMaxLength/iMaxSplit;

        Map<Integer, String> sMap = new HashMap<>();

        for(int a=0; a<iMaxSplit; a++){

            StringBuilder sb = new StringBuilder();
            for (Sentence sentence : JCasUtil.selectCovered(pCas, Sentence.class, 0, iSplitIterator * (a+1))) {
                sb.append(sentence.getCoveredText());
            }
            sMap.put(sb.toString().length(), sb.toString());

        }

        Set<Integer> lengthSet = new HashSet<>();
        lengthSet.add(140);
        lengthSet.add(250);
        lengthSet.add(500);
        lengthSet.add(750);
        lengthSet.add(1000);
        lengthSet.add(5000);
        lengthSet.add(10000);

        for (Integer iLength : lengthSet) {
            StringBuilder sb = new StringBuilder();
            for (Sentence sentence : JCasUtil.selectCovered(pCas, Sentence.class, 0, iLength)) {
                sb.append(sentence.getCoveredText());
            }
            sMap.put(sb.toString().length(), sb.toString());

        }

        sMap.put(pCas.getDocumentText().length(), pCas.getDocumentText());

        System.out.println(sMap);

        sMap.keySet().stream().forEach(k->{
            try {
                FileUtils.writeContent(sMap.get(k), new File(sOutputPath+"/sample_"+k+".txt"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }


    @Test
    public void SwarmTest() throws Exception {

        System.out.println("Running SwarmTest");

        String sInputPath = TestDUUI.class.getClassLoader().getResource("sample").getPath();
        String sSuffix = "xmi.gz";
        String sOutputPath = "/tmp/";

        int iWorkers = Integer.valueOf(2);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(ctx);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver);

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/languagedetection:0.1")
                        .withScale(iWorkers)
                        .build());

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                        .withScale(iWorkers)
                        .build());

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/gnfinder:latest")
                        .withScale(iWorkers)
                        .build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                        XmiWriter.PARAM_PRETTY_PRINT, true,
                        XmiWriter.PARAM_OVERWRITE, true,
                        XmiWriter.PARAM_VERSION, "1.1",
                        XmiWriter.PARAM_COMPRESSION, "GZIP"
                )).build());


        CollectionReaderDescription reader = null;

        reader = createReaderDescription(XmiReader.class,
                XmiReader.PARAM_SOURCE_LOCATION, sInputPath + "/**" + sSuffix,
                XmiReader.PARAM_SORT_BY_SIZE, true
        );

        composer.run(reader);


    }

    @Test
    public void TestTaxoNERD() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt dies ist ein Abies!");
        jc.setDocumentLanguage("de");

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //       .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(remote_driver);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
                        .withScale(1)
                        .build());

        composer.run(jc);

        JCasUtil.select(jc, Taxon.class).forEach(t -> {
            System.out.println(t);
        });


    }

    @Test
    public void TestBFSRL() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt dies ist ein Abies!");
        jc.setDocumentLanguage("de");

        int iWorkers = 1;

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        DUUIComposer composer = new DUUIComposer()
                //       .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIDockerDriver docker_driver = new DUUIDockerDriver(10000);
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(docker_driver);
        composer.addDriver(remote_driver);

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4").withScale(iWorkers).withImageFetching());
//        composer.add(new DUUIDockerDriver.Component("textimager_duui_bfsrl:0.0.1").withScale(iWorkers));

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9715")
                .withScale(iWorkers));

        composer.run(jc);

        JCasUtil.selectAll(jc).forEach(t -> {
            System.out.println(t);
        });


    }

    @Test
    public void LanguageDetection() throws Exception {
        JCas jc = JCasFactory.createJCas();

        // load content into jc
        // ...
        jc.setDocumentText("Hallo Welt dies ist ein Abies!");
//        jc.setDocumentLanguage("de");

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer().withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver()
                .withTimeout(10000);
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(dockerDriver, remoteDriver);

//        composer.add(new DUUIDockerDriver.Component("languagedetection:dev")
//                        .withScale(1)
//                , DUUIDockerDriver.class);
//
        composer.add(new DUUIRemoteDriver.Component("http://localhost:9714")
                        .withScale(1)
                        .build());

        composer.run(jc);

        System.out.println(jc.getDocumentLanguage());

    }

    @Test
    public void CollectionReader() throws Exception {

        String sSuffix = ".txt";
        String sInputPath = "/home/gabrami/Downloads/BioFIDExample/in/txt";
        String sOutputPath = "/home/gabrami/Downloads/BioFIDExample/out";

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer().withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver()
                .withTimeout(10000);
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(dockerDriver, remoteDriver, uimaDriver);

//        composer.add(new DUUIDockerDriver.Component("languagedetection:dev")
//                        .withScale(1)
//                , DUUIDockerDriver.class);
//
        composer.add(new DUUIUIMADriver.Component(
                createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                        XmiWriter.PARAM_PRETTY_PRINT, true,
                        XmiWriter.PARAM_OVERWRITE, true,
                        XmiWriter.PARAM_VERSION, "1.1",
                        XmiWriter.PARAM_COMPRESSION, "GZIP"
                )).withScale(1).build());


        if (sSuffix.contains("txt")) {
            composer.run(createReaderDescription(TextReader.class,
                    TextReader.PARAM_SOURCE_LOCATION, sInputPath + "/**" + sSuffix,
                    TextReader.PARAM_LANGUAGE, "de"
            ));
        } else {
            composer.run(createReaderDescription(XmiReader.class,
                    XmiReader.PARAM_SOURCE_LOCATION, sInputPath + "/**" + sSuffix,
                    XmiReader.PARAM_SORT_BY_SIZE, true,
                    XmiReader.PARAM_ADD_DOCUMENT_METADATA, true
            ));
        }


    }

    @Test
    public void RegistryTest() throws Exception {
        JCas jc = JCasFactory.createJCas();

        // load content into jc
        // ...
        jc.setDocumentText("Hallo Welt dies ist ein Abies!");
//        jc.setDocumentLanguage("de");

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer()
                //       .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIDockerDriver driver = new DUUIDockerDriver()
                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(driver, remote_driver, uima_driver, swarm_driver);

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/languagedetection:0.1")
                        .withImageFetching()
                        .withScale(1)
                        .build());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                        .withImageFetching()
                        .withScale(1)
                        .build());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/gnfinder:latest")
                        .withImageFetching()
                        .withScale(1)
                        .build());


        composer.run(jc);
        System.out.println(jc.getDocumentLanguage());
        JCasUtil.select(jc, NamedEntity.class).stream().forEach(f -> {
            System.out.println(f);
        });

    }

    @Test
    public void LuaBaseTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc, out, null);
        System.out.println(out.toString());
    }

    @Test
    public void LuaLibTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_json.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc, out, null);
        System.out.println(out.toString());
    }

    @Test
    public void LuaLibTestSandboxInstructionOverflow() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/only_loaded_classes.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox((new DUUILuaSandbox())
                .withLimitInstructionCount(1));
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        assertThrows(RuntimeException.class, () -> {
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        });
    }

    @Test
    public void LuaLibTestSandboxInstructionOk() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/only_loaded_classes.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox((new DUUILuaSandbox())
                .withLimitInstructionCount(10000));
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc, out, null);
        assertEquals(out.toString(), "");
    }

    @Test
    public void LuaLibTestSandboxForbidLoadJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox());
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class, () -> {
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        });
    }

    @Test
    public void LuaLibTestSandboxForbidLoadJavaIndirectCall() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox());
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class, () -> {
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            lua.serialize(jc, out, null);
        });
    }

    @Test
    public void LuaLibTestSandboxEnableLoadJavaIndirectCall() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllJavaClasses(true));
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc, out, null);
    }

    @Test
    public void LuaLibTestSandboxSelectiveJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllowedJavaClass("org.apache.uima.cas.impl.XmiCasSerializer")
                .withAllowedJavaClass("java.lang.String"));
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc, out, null);
    }

    @Test
    public void LuaLibTestSandboxFailureSelectiveJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllowedJavaClass("org.apache.uima.cas.impl.XmiCasSerializer"));
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class, () -> {
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        });
    }

    @Test
    public void TestSelectCovered() throws UIMAException, URISyntaxException, IOException, CompressorException, SAXException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt! Wie geht es dir?");
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);
        for(Sentence i : JCasUtil.select(jc,Sentence.class)) {
            System.out.println(JCasUtil.selectCovered(Token.class, i).stream().collect(Collectors.toList()));
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/select_covered.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out,null);
        System.out.println(out.toString());
    }

    @Test
    public void LuaLargeSerialize() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_json.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc, out, null);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua JSON in %d ms time," +
                " total bytes %d\n", end - start, out.toString().length());
        JSONArray arr = new JSONArray(out.toString());

        assertEquals(expectedNumberOfTokens, arr.getJSONArray(1).length());
        assertEquals(expectedNumberOfTokens, JCasUtil.select(jc, Token.class).size());
    }

    @Test
    public void LuaLargeSerializeMsgpack() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);


        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc, out, null);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n", end - start, out.toString().length(), expectedNumberOfTokens);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        String text = unpacker.unpackString();
        int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        assertEquals(expectedNumberOfTokens * 2, numTokensTimes2_2);
    }

    @Test
    public void JavaXMLSerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        long time = System.nanoTime();
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc,
                AnalysisEngineFactory.createEngineDescription(OpenNlpPosTagger.class));
        long endtime = System.nanoTime() - time;
        System.out.printf("Annotator time %d ms\n", (endtime) / 1000000);

        time = System.nanoTime();
        int tokens = 0;
        for (Annotation i : JCasUtil.selectCovered(jc, Annotation.class, 0, jc.getDocumentText().length())) {
            tokens += 1;
        }
        endtime = System.nanoTime() - time;
        System.out.printf("Select covered %d us\n", (endtime) / 1000);
        System.out.printf("Select covered tokens %d\n", tokens);
        int last = 0;
        time = System.nanoTime();
        int total = 0;
        int sentences = 0;
        for (Sentence i : JCasUtil.select(jc, Sentence.class)) {
            for (Token x : JCasUtil.selectCovered(Token.class, i)) {
                total += 1;
            }
            sentences += 1;
        }
        endtime = System.nanoTime() - time;
        System.out.printf("Select covered tokens %d us, sentences %d tokens %d\n", (endtime) / 1000, sentences, total);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        XmlCasSerializer.serialize(jc.getCas(), null, out);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize full XML in %d ms time," +
                " total bytes %d\n", end - start, out.toString().length());
        Files.write(Path.of("python_benches", "large_xmi.xml"), out.toByteArray());
    }

    @Test
    public void JavaBinarySerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        CasIOUtils.save(jc.getCas(), out, SerialFormat.BINARY);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize binary JCas in %d ms time," +
                " total bytes %d\n", end - start, out.toString().length());
    }

    @Test
    public void JavaSerializeMsgpack() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packString(jc.getDocumentText());
        packer.packArrayHeader(JCasUtil.select(jc, Token.class).size() * 2);
        for (Token t : JCasUtil.select(jc, Token.class)) {
            packer.packInt(t.getBegin());
            packer.packInt(t.getEnd());
        }
        packer.close();
        out.write(packer.toByteArray());

        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Java MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n", end - start, out.toString().length(), expectedNumberOfTokens);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        String text = unpacker.unpackString();
        int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        assertEquals(expectedNumberOfTokens * 2, numTokensTimes2_2);
    }

    @Test
    public void JavaSerializeJSON() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        JSONArray begin = new JSONArray();
        JSONArray endt = new JSONArray();

        for (Token t : JCasUtil.select(jc, Token.class)) {
            begin.put(t.getBegin());
            endt.put(t.getEnd());
        }
        JSONArray arr2 = new JSONArray();
        arr2.put(jc.getDocumentText());
        arr2.put(begin);
        arr2.put(endt);
        out.write(arr2.toString().getBytes(StandardCharsets.UTF_8));
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Java JSON in %d ms time," +
                " total bytes %d, total tokens %d\n", end - start, out.toString().length(), expectedNumberOfTokens);
        JSONArray arr = new JSONArray(out.toString());
        assertEquals(expectedNumberOfTokens, arr.getJSONArray(1).length());
        assertEquals(expectedNumberOfTokens, JCasUtil.select(jc, Token.class).size());
    }

    @Test
    public void LuaMsgPackNative() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("nativem", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/MessagePack.lua").toURI());
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack_native.lua").toURI()));

        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc, out, null);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua Native MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n", end - start, out.toString().length(), expectedNumberOfTokens);
    }

    @Test
    public void ComposerTest() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc2.setDocumentLanguage("de");
        AnalysisEngineDescription desc2 = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        DUUIComposer composer = new DUUIComposer();
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(desc2).build());

        composer.run(jc2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(), out);
        XmiCasSerializer.serialize(jc2.getCas(), out2);
        assertEquals(out.toString(), out2.toString());
        composer.shutdown();
    }

    @Test
    public void ComposerTestStorage() throws Exception {
        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc2.setDocumentLanguage("de");
        AnalysisEngineDescription desc2 = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        DUUIMockStorageBackend mock = new DUUIMockStorageBackend();
        DUUIComposer composer = new DUUIComposer().withStorageBackend(mock);
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(desc2).build());

        composer.run(jc2, "hallo");

        assertEquals(mock.getRunMap().contains("hallo"), true);
        assertEquals(mock.getPerformanceMonitoring().size(), 1);
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTest() throws Exception {
        DUUIMockStorageBackend mock = new DUUIMockStorageBackend();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer().withStorageBackend(mock).withLuaContext(ctx).withWorkers(4);
        composer.addDriver(new DUUIUIMADriver());
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
                .withScale(4).build());
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(4)
                .withImageFetching()
                .build());
       composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/test_benchmark/",
                        XmiWriter.PARAM_PRETTY_PRINT, true,
                        XmiWriter.PARAM_OVERWRITE, true,
                        XmiWriter.PARAM_VERSION, "1.1"
                        )
        ).withScale(4).build());

        composer.run(CollectionReaderFactory.createReaderDescription(TextReader.class,
                TextReader.PARAM_LANGUAGE,"de",
                TextReader.PARAM_SOURCE_LOCATION,"/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/output/*.txt"),"run2");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestPythonJava() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization.db");

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx);
        composer.addDriver(new DUUIRemoteDriver());

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9716").build());

        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,"/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed/*.xmi"),"run_serialize_json");
        composer.shutdown();
    }

    @Test
    public void ComposerPerformanceTestSpacy() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization.db");


        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer().withStorageBackend(sqlite).withLuaContext(ctx);
        //composer.addDriver(new DUUIRemoteDriver());
        composer.addDriver(new DUUIUIMADriver());

      /*  composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                        .withImageFetching()
                , DUUIDockerDriver.class);*/
        composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
                .build());


        composer.run(CollectionReaderFactory.createReaderDescription(TextReader.class,
                TextReader.PARAM_LANGUAGE,"de",
                TextReader.PARAM_SOURCE_LOCATION,"/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/output/*.txt"),"run_test");
        composer.shutdown();
    }

    @Test
    public void TestReproducibleAnnotations() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist ein test Text.");
        jc.setDocumentLanguage("de");

        DUUIComposer composer = new DUUIComposer();
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)).build());
        composer.run(jc,"pipeline");
        composer.shutdown();

        DUUIPipelineDescription desc = DUUIPipelineDescription.fromJCas(jc);
        assertEquals(desc.getComponents().size(),1);
        DUUIPipelineAnnotationComponent comp = desc.getComponents().get(0);

        assertEquals(comp.getComponent().getDriver(),DUUIUIMADriver.class.getCanonicalName());
        assertEquals(comp.getComponent().asUIMADriverComponent().getAnnotatorName(),BreakIteratorSegmenter.class.getCanonicalName());
    }

    @Test
    public void TestReproducibleAnnotationsDuplicateMultipleOrder() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist ein test Text.");
        jc.setDocumentLanguage("de");

        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)).build());
            composer.run(jc, "pipeline");
            composer.shutdown();
        }
        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(OpenNlpPosTagger.class)).build());
            composer.run(jc, "pos_tagger");
            composer.shutdown();
        }

        DUUIPipelineDescription desc = DUUIPipelineDescription.fromJCas(jc);

        assertEquals(desc.getComponents().size(),2);
        DUUIPipelineAnnotationComponent comp = desc.getComponents().get(0);

        assertEquals(comp.getComponent().getDriver(),DUUIUIMADriver.class.getCanonicalName());
        assertEquals(comp.getComponent().asUIMADriverComponent().getAnnotatorName(),BreakIteratorSegmenter.class.getCanonicalName());
//        assertEquals(comp.getAnnotation().getPipelineName(),"pipeline");

        DUUIPipelineAnnotationComponent comp2 = desc.getComponents().get(1);

        assertEquals(comp2.getComponent().getDriver(),DUUIUIMADriver.class.getCanonicalName());
        assertEquals(comp2.getComponent().asUIMADriverComponent().getAnnotatorName(),OpenNlpPosTagger.class.getCanonicalName());
//        assertEquals(comp2.getAnnotation().getPipelineName(),"pos_tagger");

        JCas jc_dup = JCasFactory.createJCas();
        jc_dup.setDocumentText("Dies ist ein test Text.");
        jc_dup.setDocumentLanguage("de");

        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(DUUIPipelineDescription.fromJCas(jc));
            composer.run(jc_dup, "pos_tagger");
            composer.shutdown();
        }

        assertEquals(JCasUtil.select(jc_dup,TOP.class).size(), JCasUtil.select(jc,TOP.class).size());
    }

    @Test
    public void TestReproducibleAnnotationsDuplicateChangeParameter() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist ein test's Text.");
        jc.setDocumentLanguage("de");

        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)).build());
            composer.run(jc, "pipeline");
            composer.shutdown();
        }

        DUUIPipelineDescription desc = DUUIPipelineDescription.fromJCas(jc);

        for(DUUIPipelineAnnotationComponent comp : desc.getComponents()) {
            if(comp.getComponent().asUIMADriverComponent().getAnnotatorName().equals(BreakIteratorSegmenter.class.getCanonicalName())) {
                comp.getComponent()
                        .asUIMADriverComponent()
                        .setAnalysisEngineParameter(BreakIteratorSegmenter.PARAM_SPLIT_AT_APOSTROPHE,true);
            }
        }

        JCas jc_dup = JCasFactory.createJCas();
        jc_dup.setDocumentText("Dies ist ein test's Text.");
        jc_dup.setDocumentLanguage("de");

        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(desc);
            composer.run(jc_dup, "pos_tagger");
            composer.shutdown();
        }

        assertEquals(JCasUtil.select(jc_dup,TOP.class).size(), JCasUtil.select(jc,TOP.class).size()+1);
    }

    @Test
    public void PaperExample() throws Exception {
        // A new CAS document is defined.
        // load content into jc ...
        // Defining LUA-Context for communication
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("fuchs.db")
                .withConnectionPoolSize(1);

        DUUILuaContext ctx = LuaConsts.getJSON();
        // The composer is defined and initialized with a standard Lua context.
        DUUIComposer composer = new DUUIComposer().withLuaContext(ctx)
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withWorkers(2);
        // Instantiate drivers with options
        DUUIDockerDriver docker_driver = new DUUIDockerDriver()
            .withTimeout(10000);
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        // Definition of the UIMA driver with the option of debugging output in the log.
        DUUIUIMADriver uima_driver = new DUUIUIMADriver().withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(docker_driver, remote_driver, uima_driver,
                swarm_driver);
        // Now the composer is able to use the individual drivers.
        // A new component for the composer is added
        composer.add(new DUUIDockerDriver
                // The component is based on a Docker image stored in a remote repository.
                        .Component("docker.texttechnologylab.org/gnfinder:latest")
        // The image is reloaded and fetched, regardless of whether it already exists locally (optional)
                        .withImageFetching()
        // The scaling parameter is set
        .withScale(1));
        // Adding a UIMA annotator for writing the result of the pipeline as XMI files in compressed form.
        composer.add(new DUUIUIMADriver.Component(
                createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "output_temp_path",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
                )).withScale(1));
        // The document is processed through the pipeline.
        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,"/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed_sample/*.xmi"),"run_python_token_annotator");
    }
   /* @Test
    public void TestCasIoUtils() throws UIMAException, IOException {
        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc2.setDocumentLanguage("de");
        SimplePipeline.runPipeline(jc2,AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CasIOUtils.save(jc2.getCas(),out,SerialFormat.SERIALIZED);

        JCas jc_view = JCasFactory.createJCas();
        jc_view.createView("second");
        CasIOUtils.load(new ByteArrayInputStream(out.toByteArray()), jc_view.getView("second"));
    }*/

//    @Test
//    public void XMIWriterTest() throws ResourceInitializationException, IOException, SAXException {
//
//        int iWorkers = 8;
//
//        DUUIComposer composer = new DUUIComposer().withWorkers(iWorkers);
//
//        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
//
//        composer.addDriver(uima_driver);
//
//        // UIMA Driver handles all native UIMA Analysis Engine Descriptions
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(StanfordPosTagger.class)
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(StanfordParser.class)
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(StanfordNamedEntityRecognizer.class)
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//                        XmiWriter.PARAM_TARGET_LOCATION, "/tmp/output/",
//                        XmiWriter.PARAM_PRETTY_PRINT, true,
//                        XmiWriter.PARAM_OVERWRITE, true,
//                        XmiWriter.PARAM_VERSION, "1.1",
//                        XmiWriter.PARAM_COMPRESSION, "GZIP"
//                        )
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//
//        try {
//            composer.run(createReaderDescription(XmiReaderModified.class,
//                    XmiReader.PARAM_SOURCE_LOCATION, "/resources/public/abrami/Zobodat/xmi/txt/**.xmi.gz",
//                    XmiWriter.PARAM_OVERWRITE, false
//                    //XmiReader.PARAM_LANGUAGE, LanguageToolSegmenter.PARAM_LANGUAGE)
//            ));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }

}
