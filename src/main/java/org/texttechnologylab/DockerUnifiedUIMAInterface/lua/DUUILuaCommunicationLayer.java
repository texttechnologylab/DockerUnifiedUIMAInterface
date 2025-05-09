package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.CoerceLuaToJava;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIHttpRequestHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
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

    @Override
    public void process(JCas jCas, DUUIHttpRequestHandler handler, Map<String, String> parameters) throws CommunicationLayerException, CASException {
        try {
            _file.call(
                    "process",
                    CoerceJavaToLua.coerce(jCas),
                    CoerceJavaToLua.coerce(handler),
                    createLuaTableFromParameters(parameters)
            );
        } catch (LuaError e) {
            throw new CommunicationLayerException("Caught LuaError while calling process(sourceCas, handler, parameters)", e);
        }
    }

    @Override
    public void process(JCas sourceCas, DUUIHttpRequestHandler handler, Map<String, String> parameters, JCas targetCas) throws CommunicationLayerException, CASException {
        try {
            _file.call(
                    "process",
                    CoerceJavaToLua.coerce(sourceCas),
                    CoerceJavaToLua.coerce(handler),
                    createLuaTableFromParameters(parameters),
                    CoerceJavaToLua.coerce(targetCas)
            );
        } catch (LuaError e) {
            throw new CommunicationLayerException("Caught LuaError while calling process(sourceCas, handler, parameters, targetCas)", e);
        }
    }

    public boolean supportsProcess() {
        return _file.hasGlobal("SUPPORTS_PROCESS") && _file.getGlobal("SUPPORTS_PROCESS").toboolean();
    }

    public boolean supportsSerialize() {
        return (
                _file.hasGlobal("SUPPORTS_SERIALIZE") && _file.getGlobal("SUPPORTS_SERIALIZE").toboolean()
        ) || (
                // backwards compatability: check for globally defined serialize and deserialize functions
                _file.hasGlobalFunc("serialize") && _file.hasGlobalFunc("deserialize")
        );
    }

    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String, String> parameters, String sourceView) throws CommunicationLayerException, CASException {
        LuaTable params = createLuaTableFromParameters(parameters);

        try {
            _file.call("serialize", CoerceJavaToLua.coerce(jc.getView(sourceView)), CoerceJavaToLua.coerce(out), params);
        } catch (LuaError e) {
            throw new CommunicationLayerException("Caught LuaError while calling serialize(sourceCas, outputStream, parameters)", e);
        }
    }

    public void deserialize(JCas jc, ByteArrayInputStream input) throws CommunicationLayerException, CASException {
        deserialize(jc, input, "_InitialView");
    }

    public void deserialize(JCas jc, ByteArrayInputStream input, String targetView) throws CommunicationLayerException, CASException {

        JCas tJc;

        try{
            tJc = jc.getView(targetView);
        }catch (Exception e){
            tJc = jc.createView(targetView);
        }

        try {
            _file.call("deserialize", CoerceJavaToLua.coerce(tJc), CoerceJavaToLua.coerce(input));
        } catch (LuaError e) {
            throw new CommunicationLayerException("Caught LuaError while calling deserialize(targetCas, inputStream)", e);
        }
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


    private static LuaTable createLuaTableFromParameters(Map<String, String> parameters) {
        LuaTable params = new LuaTable();
        if (parameters != null) {
            for (String key : parameters.keySet()) {
                params.set(key, parameters.get(key));
            }
        }
        return params;
    }
}
