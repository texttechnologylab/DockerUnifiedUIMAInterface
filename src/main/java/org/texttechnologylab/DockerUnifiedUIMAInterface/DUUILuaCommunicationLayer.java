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
    private Globals _globals;
    private LuaValue _serialize;
    private LuaValue _deserialize;
    private String _script;
    private String _origin;
    private DUUILuaContext _globalContext;

    public DUUILuaCommunicationLayer(String script, String origin, DUUILuaContext globalContext) {
        _script = script;
        _origin = origin;
        _globals = JsePlatform.standardGlobals();
        _globalContext = globalContext;
        for(Map.Entry<String,String> val : globalContext.getGlobalScripts().entrySet()) {
            LuaValue valsec = _globals.load(val.getValue(),"global_script"+val.getKey(),_globals);
            _globals.set(val.getKey(),valsec.call());
            //_globals.get("package").get("preload").set(val.getKey(), valsec);
        }
        LuaValue chunk = _globals.load(script, origin+"remote_lua_script",_globals);
        chunk.call();
        _serialize = _globals.get("serialize");
        _deserialize = _globals.get("deserialize");
    }

    public void serialize(JCas jc, OutputStream out) throws CompressorException, IOException, SAXException {
        _serialize.call(CoerceJavaToLua.coerce(jc),CoerceJavaToLua.coerce(out));
    }

    public void deserialize(JCas jc, InputStream input) throws IOException, SAXException {
        _deserialize.call(CoerceJavaToLua.coerce(jc),CoerceJavaToLua.coerce(input));
    }

    public IDUUICommunicationLayer copy() {
        return new DUUILuaCommunicationLayer(_script,_origin,_globalContext);
    }
}
