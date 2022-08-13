package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.ByteArrayInputStream;
import java.util.List;

public interface IDUUIConnectionHandler {
//    void initiate(String uri) throws URISyntaxException;
//
//    boolean success();
//
//    byte[] sendAwaitResponse(byte[] serializedObject) throws IOException;

    Object getClient();

    List<ByteArrayInputStream> get(byte[] jc) throws InterruptedException;

    void close();
}
