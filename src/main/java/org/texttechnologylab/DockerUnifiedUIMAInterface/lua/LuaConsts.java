package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URISyntaxException;

public class LuaConsts {

    public static DUUILuaContext getJSON() throws URISyntaxException, IOException {
        return new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
    }

}
