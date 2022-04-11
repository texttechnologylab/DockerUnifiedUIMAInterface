package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Standard scripts for LUA communication
 * @author Giuseppe Abrami
 */
public class DUUILuaCommunicationScript {

    /**
     *
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static String getUIMAXMICommunicationScript() throws URISyntaxException, IOException {
        String sReturn = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));

        return sReturn;
    }

    /**
     *
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static String getUIMAXMICommunicationJSONScript() throws URISyntaxException, IOException {
        String sReturn = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_json.lua").toURI()));

        return sReturn;
    }

    /**
     *
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static String getOnlyLoadedClasses() throws URISyntaxException, IOException {
        String sReturn = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/only_loaded_classes.lua").toURI()));

        return sReturn;
    }

    /**
     *
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static String getLUAJSON() throws URISyntaxException, IOException {
        String sReturn = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI()));

        return sReturn;
    }

}
