package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import java.io.IOException;
import java.net.URISyntaxException;

public class LuaConsts {

    public static DUUILuaContext getJSON() throws URISyntaxException, IOException {
        return new DUUILuaContext().withJsonLibrary();
        //        return new DUUILuaContext().withGlobalLibrary("json", LuaConsts.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
//        return new DUUILuaContext().withGlobalLibrary("json", new File("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
    }

}
