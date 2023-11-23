package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.luaj.vm2.*;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.*;
import org.luaj.vm2.lib.jse.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for managing and using Lua contexts
 *
 * @author Alexander Leonhardt
 */
public class DUUILuaContext {
    private Map<String,String> _luaScripts;
    private DUUILuaSandbox _sandbox;
    public DUUILuaContext() {
        _sandbox = null;
        _luaScripts = new HashMap<>();
    }

    public DUUILuaContext withGlobalLibrary(String globalName, URI path) throws IOException {
        System.out.println(path);
        _luaScripts.put(globalName,Files.readString(Path.of(path)));
        return this;
    }

    public DUUILuaContext withJsonLibrary() throws IOException {
        return this.withGlobalLibrary(
                "json",
                DUUILuaContext.class.getClassLoader().getResource(
                        "org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua"
                )
                .openStream()
        );
    }

    public DUUILuaContext withSandbox(DUUILuaSandbox sandbox) {
        _sandbox = sandbox;
        return this;
    }

    public DUUILuaContext withGlobalLibrary(String globalName, InputStream module) throws IOException {
        _luaScripts.put(globalName,new String(module.readAllBytes()));
        return this;
    }

    public DUUILuaContext withGlobalLibrary(String globalName, Path module_path) throws IOException {
        _luaScripts.put(globalName,Files.readString(module_path));
        return this;
    }

    public DUUILuaCompiledFile compileFile(String file) {
        if(_sandbox==null) {
            Globals globals = new Globals();
            globals.load(new JseBaseLib());
            globals.load(new PackageLib());
            globals.load(new Bit32Lib());
            globals.load(new TableLib());
            globals.load(new StringLib());
            globals.load(new JseMathLib());
            //globals.load(new DebugLib());
            globals.load(new CoroutineLib());
            globals.load(new JseIoLib());
            globals.load(new JseOsLib());
            globals.load(new LuajavaLib());
            LoadState.install(globals);
            LuaC.install(globals);

            for (Map.Entry<String, String> val : _luaScripts.entrySet()) {
                LuaValue valsec = globals.load(val.getValue(), "global_script" + val.getKey(), globals);
                globals.set(val.getKey(), valsec.call());
            }
            LuaValue chunk = globals.load(file, "main",globals);
            chunk.call();
            return new DUUILuaCompiledFile(globals, null,null);
        }
        else {
            Globals user_globals = new Globals();
            user_globals.load(new JseBaseLib());
            user_globals.load(new PackageLib());
            user_globals.load(new Bit32Lib());
            user_globals.load(new TableLib());
            user_globals.load(new StringLib());
            user_globals.load(new JseMathLib());
            user_globals.load(new DebugLib());

            if(_sandbox.getEnabledCoroutines()) {
                user_globals.load(new CoroutineLib());
            }

            if(_sandbox.getEnabledIo()) {
                user_globals.load(new JseIoLib());
            }

            if(_sandbox.getEnabledOs()) {
                user_globals.load(new JseOsLib());
            }

            if(_sandbox.getEnabledAllJavaClasses()) {
                user_globals.load(new LuajavaLib());
            }
            else {
                user_globals.load(new DUUICustomRestricedLuaJavaLib(_sandbox.getAllowedJavaClasses()));
            }
            LoadState.install(user_globals);
            LuaC.install(user_globals);
            LuaValue sethook = user_globals.get("debug").get("sethook");

            user_globals.set("debug", LuaValue.NIL);
            for (Map.Entry<String, String> val : _luaScripts.entrySet()) {
                LuaValue valsec = user_globals.load(val.getValue(), "global_script" + val.getKey(), user_globals);
                user_globals.set(val.getKey(), valsec.call());
            }

            LuaValue chunk = user_globals.load(file, "main", user_globals);
            LuaThread thread = new LuaThread(user_globals, chunk);
            LuaValue hookfunc = new ZeroArgFunction() {
                public LuaValue call() {
                    throw new Error("Script overran resource while compiling");
                }
            };
            sethook.invoke(LuaValue.varargsOf(new LuaValue[] { thread, hookfunc,
                    LuaValue.EMPTYSTRING, LuaValue.valueOf(_sandbox.getMaxInstructionCount()) }));

            Varargs result = thread.resume(LuaValue.NIL);
            if(!result.arg1().toboolean()) {
                throw new RuntimeException(result.arg(2).tojstring());
            }
            return new DUUILuaCompiledFile(user_globals, sethook,_sandbox);
        }
    }

    public Map<String,String> withGlobalScripts() {
        return _luaScripts;
    }
}
