import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUILuaSandbox;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DUUITest {
    @Test
    public void LuaBaseTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
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
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
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
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
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
            OutputStream out = new ByteArrayOutputStream();
            lua.serialize(jc,out);
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
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
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
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
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
