package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * Interface for socket connections
 *
 * @author Dawit Terefe, Givara Ebo
 */
@Deprecated
public interface IDUUIConnectionHandler {

    List<ByteArrayInputStream> send(byte[] jc);

    void close();
}
