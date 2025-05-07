import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.text.TextReader;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.dkpro.core.opennlp.OpenNlpPosTagger;
import org.dkpro.core.tokit.BreakIteratorSegmenter;
import org.json.JSONArray;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIPipelineAnnotationComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIPipelineDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIMultimodalCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIYouTubeReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer.AudioSegmentWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIMockStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.MultimodalUtil;
import org.texttechnologylab.annotation.type.*;
import org.texttechnologylab.utilities.helper.FileUtils;
import org.texttechnologylab.utilities.helper.RESTUtils;
import org.xml.sax.SAXException;

import javax.script.*;
import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestDUUI {

    @Test
    public void LuaBaseTest() throws Exception {
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
    public void LuaLibTest() throws Exception {
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
    public void LuaLibTestSandboxInstructionOverflow() throws Exception {
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
    public void LuaLibTestSandboxInstructionOk() throws Exception {
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
    public void LuaLibTestSandboxForbidLoadJavaClasses() throws Exception {
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
    public void LuaLibTestSandboxForbidLoadJavaIndirectCall() throws Exception {
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
    public void LuaLibTestSandboxEnableLoadJavaIndirectCall() throws Exception {
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
    public void LuaLibTestSandboxSelectiveJavaClasses() throws Exception {
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
    public void LuaLibTestSandboxFailureSelectiveJavaClasses() throws Exception {
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
    public void TestSelectCovered() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt! Wie geht es dir?");
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);
        for (Sentence i : JCasUtil.select(jc, Sentence.class)) {
            System.out.println(JCasUtil.selectCovered(Token.class, i).stream().collect(Collectors.toList()));
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/select_covered.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc, out, null);
        System.out.println(out.toString());
    }

    @Test
    @Disabled("invalid syntax in Lua script")
    public void LuaLargeSerialize() throws Exception {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
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
    public void LuaLargeSerializeMsgpack() throws Exception {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);


        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        for (int i = 0; i < 10; i++) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long start = System.currentTimeMillis();
            lua.serialize(jc, out, null);
            long end = System.currentTimeMillis();
            System.out.printf("Serialize large Lua MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n", end - start, out.toString().length(), expectedNumberOfTokens);
        }
        //MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        //String text = unpacker.unpackString();
        //int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        //assertEquals(expectedNumberOfTokens * 2, numTokensTimes2_2);
    }

    @Test
    @Disabled("missing dependency: [de.tudarmstadt.ukp.dkpro.core.opennlp-model-tagger-de-maxent] version [20120616.1]")
    public void JavaXMLSerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        long time = System.nanoTime();
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc,
            createEngineDescription(OpenNlpPosTagger.class));
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
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        CasIOUtils.save(jc.getCas(), out, SerialFormat.BINARY);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize binary JCas in %d ms time," +
            " total bytes %d\n", end - start, out.toString().length());
    }

    @Test
    public void JavaSerializeMsgpack() throws Exception {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
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
    public void JavaSerializeJSON() throws Exception {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
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
    public void LuaMsgPackNative() throws Exception {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
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
    @Disabled("these two serialized XMIs can never be equal")
    public void ComposerTest() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc2.setDocumentLanguage("de");
        AnalysisEngineDescription desc2 = createEngineDescription(BreakIteratorSegmenter.class);
        DUUIComposer composer = new DUUIComposer();
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(desc2).build());

        composer.run(jc2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(), jc.getTypeSystem(), out, true, null);
        XmiCasSerializer.serialize(jc2.getCas(), jc2.getTypeSystem(), out2, true, null);
        assertEquals(out.toString(), out2.toString());
        composer.shutdown();
    }

    @Test
    public void ComposerTestStorage() throws Exception {
        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc2.setDocumentLanguage("de");
        AnalysisEngineDescription desc2 = createEngineDescription(BreakIteratorSegmenter.class);
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
    @Disabled("missing resources")
    public void ComposerPerformanceTest() throws Exception {
        DUUIMockStorageBackend mock = new DUUIMockStorageBackend();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer().withStorageBackend(mock).withLuaContext(ctx).withWorkers(4);
        composer.addDriver(new DUUIUIMADriver());
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIUIMADriver.Component(
            createEngineDescription(BreakIteratorSegmenter.class))
            .withScale(4).build());
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
            .withScale(4)
            .withImageFetching()
            .build());
        composer.add(new DUUIUIMADriver.Component(
            createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/test_benchmark/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1"
            )
        ).withScale(4).build());

        composer.run(CollectionReaderFactory.createReaderDescription(TextReader.class,
            TextReader.PARAM_LANGUAGE, "de",
            TextReader.PARAM_SOURCE_LOCATION, "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/output/*.txt"), "run2");
        composer.shutdown();
    }

    @Test
    @Disabled("missing resources")
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
            XmiReader.PARAM_LANGUAGE, "de",
            XmiReader.PARAM_ADD_DOCUMENT_METADATA, false,
            XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA, false,
            XmiReader.PARAM_LENIENT, true,
            XmiReader.PARAM_SOURCE_LOCATION, "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed/*.xmi"), "run_serialize_json");
        composer.shutdown();
    }

    @Test
    @Disabled("missing resources")
    public void ComposerPerformanceTestSpacy() throws Exception {
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("serialization.db");


        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer().withStorageBackend(sqlite).withLuaContext(ctx);
        //composer.addDriver(new DUUIRemoteDriver());
        composer.addDriver(new DUUIUIMADriver());

      /*  composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                        .withImageFetching()
                , DUUIDockerDriver.class);*/
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class))
            .build());


        composer.run(CollectionReaderFactory.createReaderDescription(TextReader.class,
            TextReader.PARAM_LANGUAGE, "de",
            TextReader.PARAM_SOURCE_LOCATION, "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/output/*.txt"), "run_test");
        composer.shutdown();
    }

    @Test
    public void TestReproducibleAnnotations() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist ein test Text.");
        jc.setDocumentLanguage("de");

        DUUIComposer composer = new DUUIComposer();
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class)).build());
        composer.run(jc, "pipeline");
        composer.shutdown();

        DUUIPipelineDescription desc = DUUIPipelineDescription.fromJCas(jc);
        assertEquals(desc.getComponents().size(), 1);
        DUUIPipelineAnnotationComponent comp = desc.getComponents().get(0);

        assertEquals(comp.getComponent().getDriver(), DUUIUIMADriver.class.getCanonicalName());
        assertEquals(comp.getComponent().asUIMADriverComponent().getAnnotatorName(), BreakIteratorSegmenter.class.getCanonicalName());
    }

    @Test
    @Disabled("missing dependency: [de.tudarmstadt.ukp.dkpro.core.opennlp-model-tagger-de-maxent] version [20120616.1]")
    public void TestReproducibleAnnotationsDuplicateMultipleOrder() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist ein test Text.");
        jc.setDocumentLanguage("de");

        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class)).build());
            composer.run(jc, "pipeline");
            composer.shutdown();
        }
        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(new DUUIUIMADriver.Component(createEngineDescription(OpenNlpPosTagger.class)).build());
            composer.run(jc, "pos_tagger");
            composer.shutdown();
        }

        DUUIPipelineDescription desc = DUUIPipelineDescription.fromJCas(jc);

        assertEquals(desc.getComponents().size(), 2);
        DUUIPipelineAnnotationComponent comp = desc.getComponents().get(0);

        assertEquals(comp.getComponent().getDriver(), DUUIUIMADriver.class.getCanonicalName());
        assertEquals(comp.getComponent().asUIMADriverComponent().getAnnotatorName(), BreakIteratorSegmenter.class.getCanonicalName());
//        assertEquals(comp.getAnnotation().getPipelineName(),"pipeline");

        DUUIPipelineAnnotationComponent comp2 = desc.getComponents().get(1);

        assertEquals(comp2.getComponent().getDriver(), DUUIUIMADriver.class.getCanonicalName());
        assertEquals(comp2.getComponent().asUIMADriverComponent().getAnnotatorName(), OpenNlpPosTagger.class.getCanonicalName());
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

        assertEquals(JCasUtil.select(jc_dup, TOP.class).size(), JCasUtil.select(jc, TOP.class).size());
    }

    @Test
    public void TestReproducibleAnnotationsDuplicateChangeParameter() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist ein test's Text.");
        jc.setDocumentLanguage("de");

        {
            DUUIComposer composer = new DUUIComposer();
            composer.addDriver(new DUUIUIMADriver());
            composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class)).build());
            composer.run(jc, "pipeline");
            composer.shutdown();
        }

        DUUIPipelineDescription desc = DUUIPipelineDescription.fromJCas(jc);

        for (DUUIPipelineAnnotationComponent comp : desc.getComponents()) {
            if (comp.getComponent().asUIMADriverComponent().getAnnotatorName().equals(BreakIteratorSegmenter.class.getCanonicalName())) {
                comp.getComponent()
                    .asUIMADriverComponent()
                    .setAnalysisEngineParameter(BreakIteratorSegmenter.PARAM_SPLIT_AT_APOSTROPHE, true);
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

        assertEquals(JCasUtil.select(jc_dup, TOP.class).size(), JCasUtil.select(jc, TOP.class).size() + 1);
    }

    @Test
    public void nashorn() throws Exception {
        ScriptEngine ee = new ScriptEngineManager().getEngineByName("Nashorn");
        CompiledScript compiled = ((Compilable) ee).compile("var token = Java.type(\"de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token\");\n" +
            "var util = Java.type(\"org.apache.uima.fit.util.JCasUtil\");\n" +
            "var msgpack = Java.type(\"org.msgpack.core.MessagePack\");" +
            "    var packer = msgpack.newDefaultPacker(outputStream);" +
            "packer.packArrayHeader(2);" +
            "packer.packString(inputCas.getDocumentText());\n" +
            "var size = util.select(inputCas,token.class).size();\n" +
            "packer.packArrayHeader(size*2);\n" +
            "var result = util.select(inputCas,token.class).iterator();\n" +
            "while(result.hasNext()) {\n" +
            "   var x = result.next();\n" +
            "    packer.packInt(x.getBegin());\n" +
            "    packer.packInt(x.getEnd());\n" +
            "}\n" +
            "  packer.close();" +
            "");


        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc, desc);

        int expectedNumberOfTokens = 0;
        for (Token t : JCasUtil.select(jc, Token.class)) {
            expectedNumberOfTokens += 1;
        }


        for (int i = 0; i < 10; i++) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long start = System.currentTimeMillis();
            Bindings b = ee.createBindings();
            b.put("outputStream", out);
            b.put("inputCas", jc);
            compiled.eval(b);
            //invocable.invokeFunction("serialize",jc, out, null);
            long end = System.currentTimeMillis();
            System.out.printf("Serialize large Nashorn MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n", end - start, out.toString().length(), expectedNumberOfTokens);
        }
    }

    @Test
    @Disabled("missing resources")
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
            XmiReader.PARAM_LANGUAGE, "de",
            XmiReader.PARAM_ADD_DOCUMENT_METADATA, false,
            XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA, false,
            XmiReader.PARAM_LENIENT, true,
            XmiReader.PARAM_SOURCE_LOCATION, "/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed_sample/*.xmi"), "run_python_token_annotator");
    }


    @Test
    @Disabled("missing resources")
    public void kubernetesTest() throws Exception {
        String sInputPath = "/inputpath";

        String sOutputPath = "outputpath";
        String sSuffix = "xmi.gz";

        // Asynchroner reader für die Input-Dateien
        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, false);
        new File(sOutputPath).mkdir();

        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(10);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIKubernetesDriver kubernetes_driver = new DUUIKubernetesDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(kubernetes_driver, uima_driver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.


        composer.add(new DUUIKubernetesDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers)
                .build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "sentence");
    }

    @Test
    @Disabled("missing resources")
    public void differentViewsTest() throws Exception{

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("hf_key.txt");

        File keyFile = new File(resource.toURI());;

        String content = "";
        try {
            content += Files.readAllLines(keyFile.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String hfKey = content.substring(1, content.length() - 1);

        JCas aCas = JCasFactory.createJCas();

        File videoFile = new File("D:/DUUIVideos/read/TBBT.mp4");
        if (videoFile.exists()) {
            String encoded = org.apache.commons.codec.binary.Base64.encodeBase64String(org.apache.commons.io.FileUtils.readFileToByteArray(videoFile));
            String mimeType = Files.probeContentType(videoFile.toPath());
            System.out.println(mimeType);
            aCas.setSofaDataString(encoded, mimeType);
            //aCas.setSofaDataString("https://www.youtube.com/watch?v=dEMzzgbm6Ow", mimeType);
        }else{
            System.out.println(videoFile.getAbsolutePath() + " not found");
            return;
        }

        //aCas.setSofaDataString("https://www.youtube.com/watch?v=8qZYsYq_Ctw", "text/x-uri");

        int iWorkers = 1;

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(uima_driver, remoteDriver, dockerDriver);

        aCas.setDocumentLanguage("de");

        /*
        composer.add(new DUUIDockerDriver.Component("duui-yt-dlp:latest")  // YT Downloader
                .withScale(iWorkers)
                .withTargetView("video_view")
                .withParameter("withTranscription", "true")
                .build());

        composer.add(new DUUIDockerDriver.Component("duui-video-to-audio:latest")  // Video to audio
                .withScale(iWorkers)
                .withSourceView("video_view")
                .withTargetView("audio_view")
                .build());

        composer.add(new DUUIDockerDriver.Component("duui-whisper:latest")  // Audio to text
                .withScale(iWorkers)
                .withSourceView("audio_view")
                .withTargetView("text_view")
                .build());

        composer.add(new DUUIDockerDriver.Component("duui-pyannote:latest")  // Audio to speaker
                .withScale(iWorkers)
                .withSourceView("audio_view")
                .withParameter("token", hfKey)
                .withTargetView("text_view")
                .build()); */

        composer.add(new DUUIDockerDriver.Component("duui-annotheia:latest")  // Annotheia
                .withScale(iWorkers)
                .withTargetView("text_view")
                .withName("annotheia")
                //.withRunningAfterDestroy(true)
                //.withParameter("device", "cuda")
                .build());

        composer.add(new DUUIDockerDriver.Component("duui-spacy:latest")  // Spacy
                .withScale(iWorkers)
                .withView("text_view")
                .withParameter("use_existing_sentences", "false")
                .withParameter("use_existing_tokens", "false")
                .build());

        /*composer.add(new DUUIRemoteDriver.Component("http://localhost:9717")  // Audio to speaker
                .withScale(iWorkers)
                .withSourceView("audio_view")
                .withTargetView("text_view")
                .withParameter("token", hfKey)
                .withParameter("device", "cuda")
                .build());
        */
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "C:/test/temp",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());

        composer.run(aCas);

        MultimodalUtil.getAllCoveredVideo(aCas.getView("text_view"), aCas, Sentence.class, "mp4").forEach(file -> {
                try {
                    org.apache.commons.io.FileUtils.moveFile(new File(file.getAbsolutePath()), new File("C:/test/" + file.getName()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        );
    }

    @Test
    @Disabled("missing resources")
    public void youtubeReaderTest() throws Exception{

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("hf_key.txt");

        File file = new File(resource.toURI());;

        String content = "";
        try {
            content += Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String hfKey = content.substring(1, content.length() - 1);

        //CasIOUtils.save(aCas.getCas(), new FileOutputStream(new File("/tmp/audiotest.xmi")), SerialFormat.XMI_1_1);
        int iWorkers = 1;

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();


        //DUUIYouTubeReader ytReader = new DUUIYouTubeReader("https://www.youtube.com/@Jules1/videos", "AIzaSyDycLCdJ1_jfkFL-pWnQuf1FzluJbX21Bw");
        //DUUIYouTubeReader ytReader = new DUUIYouTubeReader("https://www.youtube.com/watch?v=SV6NJ6PcGBs&list=PLh19WWr20745LHdlDAg2P_JT7I2Wx6axP", "AIzaSyDycLCdJ1_jfkFL-pWnQuf1FzluJbX21Bw");
        DUUIMultimodalCollectionReader multiReader = new DUUIMultimodalCollectionReader("D:/DUUIVideos/read", "mp4");

        Set<DUUICollectionReader> readers = new HashSet<>();

        //readers.add(ytReader);
        readers.add(multiReader);

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(readers);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(uima_driver, remoteDriver);

        /*composer.add(new DUUIRemoteDriver.Component("http://localhost:9713")  // Youtube downloader
                .withScale(iWorkers)
                .withTargetView("video_view")
                .withParameter("withTranscription", "false")
                .build());

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9714")  // Video to audio
                .withScale(iWorkers)
                .withSourceView("video_view")
                .withTargetView("audio_view")
                .build());

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9715")  // Audio to text
                .withScale(iWorkers)
                .withSourceView("audio_view")
                .withTargetView("text_view")
                .withParameter("device", "cuda")
                .build());*/

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9717")  // Annotheia
                .withScale(iWorkers)
                .withTargetView("text_view")
                //.withParameter("token", hfKey)
                .withParameter("device", "cuda")
                .build());

        /*composer.add(new DUUIRemoteDriver.Component("http://localhost:9720")  // Spacy
                .withScale(iWorkers)
                .withSourceView("text_view")
                .withTargetView("text_view")
                .withParameter("use_existing_sentences", "false")
                .withParameter("use_existing_tokens", "false")
                .build());*/

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "C:/test/temp",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());

        /*composer.add(new DUUIUIMADriver.Component(createEngineDescription(AudioSegmentWriter.class,
                AudioSegmentWriter.PARAM_TARGET_LOCATION, "C:/test",
                AudioSegmentWriter.PARAM_AUDIO_CONTENT_VIEW, "audio_view",
                AudioSegmentWriter.PARAM_AUDIO_TOKEN_VIEW, "text_view"))
                .build()); */


        composer.run(processor, "test");


        //composer.run(aCas);
    }

    @Test
    @Disabled("missing resources")
    public void multimodalFileReaderTest() throws Exception{


        int iWorkers = 1;

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();


        DUUIMultimodalCollectionReader multiReader = new DUUIMultimodalCollectionReader("D:/DUUIVideos/read", "gz");

        Set<DUUICollectionReader> readers = new HashSet<>();

        readers.add(multiReader);

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(readers);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(uima_driver, remoteDriver, dockerDriver);
        /*
        composer.add(new DUUIRemoteDriver.Component("http://localhost:9713")  // Youtube downloader
                .withScale(iWorkers)
                .withTargetView("video_view")
                .withParameter("withTranscription", "false")
                .build());

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9714")  // Video to audio
                .withScale(iWorkers)
                .withSourceView("video_view")
                .withTargetView("audio_view")
                .build());

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9715")  // Audio to text
                .withScale(iWorkers)
                .withSourceView("audio_view")
                .withTargetView("text_view")
                .withParameter("device", "cuda")
                .build());

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9717")  // Audio to speaker
                .withScale(iWorkers)
                .withSourceView("audio_view")
                .withTargetView("text_view")
                .withParameter("token", hfKey)
                .withParameter("device", "cuda")
                .build());*/

       /* composer.add(new DUUIDockerDriver.Component("duui-spacy:latest")  // Spacy
                .withScale(iWorkers)
                .withView("text_view")
                .withRunningAfterDestroy(true)
                .withParameter("use_existing_sentences", "false")
                .withParameter("use_existing_tokens", "false")
                .build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "C:/test/temp",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());*/

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(AudioSegmentWriter.class,
                AudioSegmentWriter.PARAM_TARGET_LOCATION, "C:/test",
                //AudioSegmentWriter.PARAM_AUDIO_CONTENT_VIEW, "text_view",
                AudioSegmentWriter.PARAM_AUDIO_TOKEN_VIEW, "text_view"))
                .build());


        composer.run(processor, "test");

        //composer.run(aCas);
    }



    @Test
    @Disabled("missing resources")
    public void multimodalImageCutterTest() throws Exception{

        JCas aCas = JCasFactory.createJCas();

        File videoFile = new File("D:/DUUIVideos/read/India_Street.jpg");
        if (videoFile.exists()) {
            String encoded = org.apache.commons.codec.binary.Base64.encodeBase64String(org.apache.commons.io.FileUtils.readFileToByteArray(videoFile));
            String mimeType = Files.probeContentType(videoFile.toPath());
            System.out.println(mimeType);
            aCas.setSofaDataString(encoded, mimeType);
        }else{
            System.out.println(videoFile.getAbsolutePath() + " not found");
            return;
        }

        int iWorkers = 1;

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(uima_driver, remoteDriver, dockerDriver);

        composer.add(new DUUIDockerDriver.Component("duui-yolo:latest")  // Image detection
                .withScale(iWorkers)
                .build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "C:/test/temp",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());

        composer.run(aCas);

        MultimodalUtil.getSubImages(aCas).forEach(file -> {
                    try {
                        org.apache.commons.io.FileUtils.moveFile(new File(file.getAbsolutePath()), new File("C:/test/" + file.getName()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        //composer.run(aCas);
    }

    @Test
    public void pdfView() throws Exception{

        JCas aCas = JCasFactory.createJCas();

        File dFile = FileUtils.downloadFile("https://aclanthology.org/2023.findings-emnlp.29.pdf");

        String encoded = org.apache.commons.codec.binary.Base64.encodeBase64String(org.apache.commons.io.FileUtils.readFileToByteArray(dFile));
            String mimeType = Files.probeContentType(dFile.toPath());
            System.out.println(mimeType);
            JCas pdfCas = aCas.createView("pdf");
            pdfCas.setSofaDataString(encoded, mimeType);

            CasIOUtils.save(aCas.getCas(), new FileOutputStream(new File("/tmp/DUUI.xmi")), SerialFormat.XMI_1_1_PRETTY);


        //composer.run(aCas);
    }

}
