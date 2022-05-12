import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XmlCasSerializer;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUILocalDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIMockStorageBackend;
import org.texttechnologylab.annotation.type.Taxon;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestDUUI {
    @Test
    public void TestTaxoNERD() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt dies ist ein Abies!");
        jc.setDocumentLanguage("de");

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //       .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(remote_driver);

        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
                        .withScale(1)
                , DUUIRemoteDriver.class);

        composer.run(jc);

        JCasUtil.select(jc, Taxon.class).forEach(t->{
            System.out.println(t);
        });




    }

    @Test
    public void RegistryTest() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt dies ist ein Abies!");
        jc.setDocumentLanguage("de");

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
                //       .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx);

        // Instantiate drivers with options
        DUUILocalDriver driver = new DUUILocalDriver()
                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(driver);
        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);
        composer.addDriver(swarm_driver);

        composer.add(new DUUILocalDriver.Component("docker.texttechnologylab.org/gnfinder:latest")
                        .withImageFetching()
                        .withScale(1)
                , DUUILocalDriver.class);

        composer.run(jc);

        int iCount = JCasUtil.selectAt(jc, Taxon.class, 24, 30).size();

        assertEquals(iCount, 2);


    }
    @Test
    public void LuaBaseTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out,null);
        System.out.println(out.toString());
    }

    @Test
    public void LuaLibTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_json.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out,null);
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
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

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
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out,null);
        assertEquals(out.toString(),"");
    }

    @Test
    public void LuaLibTestSandboxForbidLoadJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox());
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class,()->{DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);});
    }

    @Test
    public void LuaLibTestSandboxForbidLoadJavaIndirectCall() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox());
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class,()->{
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            lua.serialize(jc,out,null);
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
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out,null);
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
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out,null);
    }

    @Test
    public void LuaLibTestSandboxFailureSelectiveJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllowedJavaClass("org.apache.uima.cas.impl.XmiCasSerializer"));
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class,()-> {
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        });
    }

    @Test
    public void LuaLargeSerialize() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_json.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out,null);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua JSON in %d ms time," +
                " total bytes %d\n",end-start,out.toString().length());
        JSONArray arr = new JSONArray(out.toString());

        assertEquals(expectedNumberOfTokens,arr.getJSONArray(1).length());
        assertEquals(expectedNumberOfTokens,JCasUtil.select(jc,Token.class).size());
    }

    @Test
    public void LuaLargeSerializeMsgpack() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);


        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out,null);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        String text = unpacker.unpackString();
        int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        assertEquals(expectedNumberOfTokens*2,numTokensTimes2_2);
    }

    @Test
    public void JavaXMLSerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        XmlCasSerializer.serialize(jc.getCas(),null,out);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize full XML in %d ms time," +
                " total bytes %d\n",end-start,out.toString().length());
        Files.write(Path.of("python_benches","large_xmi.xml"),out.toByteArray());
    }

    @Test
    public void JavaBinarySerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        CasIOUtils.save(jc.getCas(),out, SerialFormat.BINARY);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize binary JCas in %d ms time," +
                " total bytes %d\n",end-start,out.toString().length());
    }

    @Test
    public void JavaSerializeMsgpack() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packString(jc.getDocumentText());
        packer.packArrayHeader(JCasUtil.select(jc,Token.class).size()*2);
        for(Token t : JCasUtil.select(jc,Token.class)) {
            packer.packInt(t.getBegin());
            packer.packInt(t.getEnd());
        }
        packer.close();
        out.write(packer.toByteArray());

        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Java MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        String text = unpacker.unpackString();
        int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        assertEquals(expectedNumberOfTokens*2,numTokensTimes2_2);
    }

    @Test
    public void JavaSerializeJSON() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        JSONArray begin = new JSONArray();
        JSONArray endt = new JSONArray();

        for(Token t : JCasUtil.select(jc,Token.class)) {
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
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
        JSONArray arr = new JSONArray(out.toString());
        assertEquals(expectedNumberOfTokens,arr.getJSONArray(1).length());
        assertEquals(expectedNumberOfTokens,JCasUtil.select(jc,Token.class).size());
    }

    @Test
    public void LuaMsgPackNative() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("nativem",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/MessagePack.lua").toURI());
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack_native.lua").toURI()));

        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out,null);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua Native MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
    }

    @Test
    public void ComposerTest() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentText("Dies ist der erste Testatz. Hier ist der zweite Testsatz!");
        jc2.setDocumentLanguage("de");
        AnalysisEngineDescription desc2 = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        DUUIComposer composer = new DUUIComposer();
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(desc2),DUUIUIMADriver.class);

        composer.run(jc2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(),out);
        XmiCasSerializer.serialize(jc2.getCas(),out2);
        assertEquals(out.toString(),out2.toString());
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
        composer.add(new DUUIUIMADriver.Component(desc2),DUUIUIMADriver.class);

        composer.run(jc2,"hallo");

        assertEquals(mock.getRunMap().contains("hallo"),true);
        assertEquals(mock.getPerformanceMonitoring().size(),1);
        composer.shutdown();
    }

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
