package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import java.io.IOException;

/**
 * Lua Consts
 *
 * @author Giuseppe Abrami
 */
public class LuaConsts {

    public static DUUILuaContext getJSON() throws IOException {
        return new DUUILuaContext().withJsonLibrary();
    }

}
