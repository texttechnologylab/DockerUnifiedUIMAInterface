package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.io.IOException;
import java.io.InputStream;
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

    public DUUILuaContext addGlobalLibrary(String globalName, InputStream module) throws IOException {
        _luaScripts.put(globalName,new String(module.readAllBytes()));
        return this;
    }

    public DUUILuaContext addGlobalLibrary(String globalName, Path module_path) throws IOException {
        _luaScripts.put(globalName,Files.readString(module_path));
        return this;
    }

    public Map<String,String> getGlobalScripts() {
        return _luaScripts;
    }
}
