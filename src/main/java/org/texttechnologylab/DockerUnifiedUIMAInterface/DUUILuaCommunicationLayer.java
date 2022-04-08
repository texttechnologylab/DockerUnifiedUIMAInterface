package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class DUUILuaCommunicationLayer implements IDUUICommunicationLayer {
    private String _script;
    private String _origin;
    private DUUILuaContext _globalContext;
    private DUUILuaCompiledFile _file;

    public DUUILuaCommunicationLayer(String script, String origin, DUUILuaContext globalContext) {
        _script = script;
        _origin = origin;
        _file = globalContext.compileFile(script);
        _globalContext = globalContext;
    }

    public void serialize(JCas jc, OutputStream out) throws CompressorException, IOException, SAXException {
        _file.call("serialize",CoerceJavaToLua.coerce(jc),CoerceJavaToLua.coerce(out));
    }

    public void deserialize(JCas jc, InputStream input) throws IOException, SAXException {
        _file.call("deserialize",CoerceJavaToLua.coerce(jc),CoerceJavaToLua.coerce(input));
    }

    public IDUUICommunicationLayer copy() {
        return new DUUILuaCommunicationLayer(_script,_origin,_globalContext);
    }
}
