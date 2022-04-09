package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaThread;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.ZeroArgFunction;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;

public class DUUILuaCompiledFile {
    private Globals _globals;
    private LuaValue _sethook;
    private DUUILuaSandbox _sandbox;

    DUUILuaCompiledFile(Globals globals, LuaValue sethook, DUUILuaSandbox sandbox) {
        _globals = globals;
        _sethook = sethook;
        _sandbox = sandbox;
    }

    LuaValue call(String funcName, LuaValue arg1, LuaValue arg2) {
        if(_sethook != null) {
            LuaThread thread = new LuaThread(_globals, _globals.get(funcName));
            LuaValue hookfunc = new ZeroArgFunction() {
                public LuaValue call() {
                    throw new Error("Script overran resource while running \""+funcName+"\"");
                }
            };
            _sethook.invoke(LuaValue.varargsOf(new LuaValue[] { thread, hookfunc,
                    LuaValue.EMPTYSTRING, LuaValue.valueOf(_sandbox.getMaxInstructionCount()) }));

            Varargs result = thread.resume(LuaValue.varargsOf(arg1,arg2));
            if(!result.arg1().toboolean()) {
                throw new RuntimeException(result.arg(2).tojstring());
            }
            return result.arg(2);
        }
        else {
            return _globals.get(funcName).call(arg1,arg2);
        }
    }
}
