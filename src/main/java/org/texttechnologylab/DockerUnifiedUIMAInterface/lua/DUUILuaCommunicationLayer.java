package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.CoerceLuaToJava;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a communication layer for LUA
 *
 * @author Alexander Leonhardt
 */
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

    public SerializeOutput serialize(JCas jc, ByteArrayOutputStream out, Map<String, String> parameters, String sourceView) throws CompressorException, IOException, SAXException, CASException {
        LuaTable params = new LuaTable();
        if (parameters != null) {
            for (String key : parameters.keySet()) {
                params.set(key, parameters.get(key));
            }
        }

        LuaValue output = _file.call("serialize", CoerceJavaToLua.coerce(jc.getView(sourceView)), CoerceJavaToLua.coerce(out), params);

        return new SerializeOutput(output);
    }

    public void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException, CASException {
        deserialize(jc, input, "_InitialView");
    }

    public void deserialize(JCas jc, ByteArrayInputStream input, String targetView) throws IOException, SAXException, CASException {

        JCas tJc;

        try{
            tJc = jc.getView(targetView);
        }catch (Exception e){
            tJc = jc.createView(targetView);
        }

        _file.call("deserialize",CoerceJavaToLua.coerce(tJc),CoerceJavaToLua.coerce(input));
    }


    public IDUUICommunicationLayer copy() {
        return new DUUILuaCommunicationLayer(_script,_origin,_globalContext);
    }

    @Override
    public ByteArrayInputStream merge(List<ByteArrayInputStream> results) {
        return (ByteArrayInputStream) CoerceLuaToJava.coerce(_file.call("merge", CoerceJavaToLua.coerce(results)), ByteArrayInputStream.class);
    }

    @Override
    public String myLuaTestMerging() {
        // Die Funktion von Lua wird hier aufgerufen
        _file.call("merging",null, null);
        return "merging.................. ";
    }


}
