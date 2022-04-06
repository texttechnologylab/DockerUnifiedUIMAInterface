package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DUUILuaContext {
    private Map<String,String> _luaScripts;
    public DUUILuaContext() {
        _luaScripts = new HashMap<>();
    }

    public DUUILuaContext addGlobalLibrary(String globalName, URI path) throws IOException {
        _luaScripts.put(globalName,Files.readString(Path.of(path)));
        return this;
    }

    public Map<String,String> getGlobalScripts() {
        return _luaScripts;
    }
}
