package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.IOException;
import java.net.URISyntaxException;

public interface IDUUIConnectionHandler {

    void initiate(String uri) throws URISyntaxException;

    boolean success();

    byte[] sendAwaitResponse(byte[] serializedObject) throws IOException;
}
