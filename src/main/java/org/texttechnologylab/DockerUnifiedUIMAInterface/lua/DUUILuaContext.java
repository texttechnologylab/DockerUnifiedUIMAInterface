package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.luaj.vm2.*;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.*;
import org.luaj.vm2.lib.jse.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUILuaLogger;

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
    /**
     * Lock held by every call into LuaJ (compiling and running a script, coercing values for it).
     *
     * <p>LuaJ 3.0.1 keeps process-wide caches in plain {@link java.util.HashMap}s that it fills while scripts run:
     * {@code CoerceJavaToLua.COERCIONS}, {@code CoerceLuaToJava.COERCIONS}, {@code JavaClass.classes},
     * {@code JavaMethod.methods} and {@code JavaConstructor.constructors} (static final, so they cannot be replaced).
     * Several composer workers running Lua scripts at the same time can corrupt them. A lost entry stays lost for the
     * rest of the process: e.g. without the entry for {@code Integer}, every {@code int} returned by a Java method
     * reaches Lua as userdata, and a script fails with "attempt to perform arithmetic __sub on userdata and number"
     * on every following document. Holding this lock lets only one thread at a time use LuaJ.
     */
    public static final Object LUAJ_LOCK = new Object();

    private final Map<String, String> _luaScripts;
    private DUUILuaSandbox _sandbox;

    public DUUILuaContext() {
        _sandbox = null;
        _luaScripts = new HashMap<>();
    }

    public DUUILuaContext withGlobalLibrary(String globalName, URI path) throws IOException {
        System.out.println(path);
        _luaScripts.put(globalName, Files.readString(Path.of(path)));
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
        _luaScripts.put(globalName, new String(module.readAllBytes()));
        return this;
    }

    public DUUILuaContext withGlobalLibrary(String globalName, Path module_path) throws IOException {
        _luaScripts.put(globalName, Files.readString(module_path));
        return this;
    }

    public DUUILuaCompiledFile compileFile(String file) {
        return compileFile(file, new DUUILuaLogger());
    }


    public DUUILuaCompiledFile compileFile(String file, DUUILuaLogger logger) {
        // running the script's top level uses LuaJ's shared caches (e.g. luajava.bindClass), see LUAJ_LOCK
        synchronized (LUAJ_LOCK) {
            return compileFileLocked(file, logger);
        }
    }

    private DUUILuaCompiledFile compileFileLocked(String file, DUUILuaLogger logger) {
        if (_sandbox == null) {
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

            // Auto import: DUUILogging to every script.
            globals.set("DUUILogging", CoerceJavaToLua.coerce(logger));

            for (Map.Entry<String, String> val : _luaScripts.entrySet()) {
                LuaValue valsec = globals.load(val.getValue(), "global_script" + val.getKey(), globals);
                globals.set(val.getKey(), valsec.call());
            }
            LuaValue chunk = globals.load(file, "main", globals);
            chunk.call();
            return new DUUILuaCompiledFile(globals, null, null);
        } else {
            Globals user_globals = new Globals();
            user_globals.load(new JseBaseLib());
            user_globals.load(new PackageLib());
            user_globals.load(new Bit32Lib());
            user_globals.load(new TableLib());
            user_globals.load(new StringLib());
            user_globals.load(new JseMathLib());
            user_globals.load(new DebugLib());

            if (_sandbox.getEnabledCoroutines()) {
                user_globals.load(new CoroutineLib());
            }

            if (_sandbox.getEnabledIo()) {
                user_globals.load(new JseIoLib());
            }

            if (_sandbox.getEnabledOs()) {
                user_globals.load(new JseOsLib());
            }

            if (_sandbox.getEnabledAllJavaClasses()) {
                user_globals.load(new LuajavaLib());
            } else {
                user_globals.load(new DUUICustomRestricedLuaJavaLib(_sandbox.getAllowedJavaClasses()));
            }
            LoadState.install(user_globals);
            LuaC.install(user_globals);
            LuaValue sethook = user_globals.get("debug").get("sethook");

            user_globals.set("debug", LuaValue.NIL);

            // Auto import: DUUILogging
            user_globals.set("DUUILogging", CoerceJavaToLua.coerce(logger));

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
            sethook.invoke(LuaValue.varargsOf(new LuaValue[]{thread, hookfunc,
                    LuaValue.EMPTYSTRING, LuaValue.valueOf(_sandbox.getMaxInstructionCount())}));

            Varargs result = thread.resume(LuaValue.NIL);
            if (!result.arg1().toboolean()) {
                throw new RuntimeException(result.arg(2).tojstring());
            }
            return new DUUILuaCompiledFile(user_globals, sethook, _sandbox);
        }
    }

    public Map<String, String> withGlobalScripts() {
        return _luaScripts;
    }
}
