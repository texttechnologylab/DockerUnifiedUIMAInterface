package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.ByteArrayInputStream;
import java.util.List;

public interface IDUUIConnectionHandler {

    List<ByteArrayInputStream> send(byte[] jc);

    void close();
}
