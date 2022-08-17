package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.jcas.JCas;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.xml.sax.SAXException;

import java.io.*;
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

    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters) throws CompressorException, IOException, SAXException {
        LuaTable params = new LuaTable();
        for(String key: parameters.keySet()) {
            params.set(key,parameters.get(key));
        }
        _file.call("serialize",CoerceJavaToLua.coerce(jc),CoerceJavaToLua.coerce(out), params);
    }

    public void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException {
        _file.call("deserialize",CoerceJavaToLua.coerce(jc),CoerceJavaToLua.coerce(input));
    }

    public IDUUICommunicationLayer copy() {
        return new DUUILuaCommunicationLayer(_script,_origin,_globalContext);
    }
}
