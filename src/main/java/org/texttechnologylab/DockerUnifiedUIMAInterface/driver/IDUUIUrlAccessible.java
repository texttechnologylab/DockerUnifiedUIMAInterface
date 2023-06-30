package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;

public interface IDUUIUrlAccessible {
    public String generateURL();
    public IDUUIConnectionHandler getHandler();
    public IDUUICommunicationLayer getCommunicationLayer();
}
