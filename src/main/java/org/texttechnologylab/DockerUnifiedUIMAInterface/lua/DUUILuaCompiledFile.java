package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.luaj.vm2.*;
import org.luaj.vm2.lib.ZeroArgFunction;

import java.util.Objects;

/**
 * Class on the use of Lua
 *
 * @author Alexander Leonhardt
 * @author Manuel Schaaf
 */
public class DUUILuaCompiledFile {
    private Globals _globals;
    private LuaValue _sethook;
    private DUUILuaSandbox _sandbox;

    DUUILuaCompiledFile(Globals globals, LuaValue sethook, DUUILuaSandbox sandbox) {
        _globals = globals;
        _sethook = sethook;
        _sandbox = sandbox;
    }

    /**
     * Call the given function with a variable number of arguments.
     *
     * @param funcName Name of the function in the LUA script to be invoked.
     * @param args     Any number of parameters to pass to the LUA function.
     * @return A {@link LuaTable} with any values returned by the invoked function
     */
    LuaValue call(String funcName, LuaValue... args) {
        if (_sethook != null) {
            return callThreaded(funcName, args);
        } else {
            /// Use {@link LuaValue#invoke(LuaValue[])} to handle any number of arguments
            return _globals.get(funcName).invoke(args).arg1();
        }
    }

    /**
     * Invokes a Lua function in a separate thread with a specified execution limit.
     *
     * @param funcName The name of the function to be executed in the Lua environment.
     * @param args     An array of LuaValue arguments to pass to the invoked function.
     * @return The {@link LuaValue} returned by the invoked function.
     * @throws RuntimeException If the thread execution fails or the Lua function throws an error.
     * @throws Error            If the Lua function exceeds the maximum allowed instruction count.
     */
    private LuaValue callThreaded(String funcName, LuaValue[] args) {
        LuaThread thread = new LuaThread(_globals, _globals.get(funcName));
        LuaValue hookfunc = new ZeroArgFunction() {
            public LuaValue call() {
                throw new Error("Script overran resource while running \"" + funcName + "\"");
            }
        };
        _sethook.invoke(LuaValue.varargsOf(new LuaValue[]{thread, hookfunc, LuaValue.EMPTYSTRING, LuaValue.valueOf(_sandbox.getMaxInstructionCount())}));

        Varargs result = thread.resume(LuaValue.varargsOf(args));
        if (!result.arg1().toboolean()) {
            throw new RuntimeException(result.arg(2).tojstring());
        }
        return result.arg(2);
    }

    /**
     * Check if there is a globally defined variable of the given name.
     *
     * @param name name of the variable to get
     * @return true if such a variable exists
     */
    public boolean hasGlobal(String name) {
        LuaValue global = _globals.get(name);
        return global != null && !global.isnil();
    }

    /**
     * Check if there is a globally defined function of the given name.
     *
     * @param name name of the function to check
     * @return true if the function exists
     */
    public boolean hasGlobalFunc(String name) {
        LuaValue global = _globals.get(name);
        return global != null && !global.isnil() && global.isfunction();
    }

    /**
     * Get the value of a globally defined symbol in the compiled Lua script.
     *
     * @param name name of the symbol to get
     * @return the globally defined {@link LuaValue}
     * @throws NullPointerException if the symbol does not exist, use {@link #hasGlobal} to check for {@code null}'s
     *
     * @see #hasGlobal
     */
    public LuaValue getGlobal(String name) {
        return Objects.requireNonNull(_globals.get(name));
    }
}
