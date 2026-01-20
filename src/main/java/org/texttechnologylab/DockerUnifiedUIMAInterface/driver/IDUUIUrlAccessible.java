package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;

/**
 * Interface for the generation of DUUI-available URLs
 *
 * @author Alexander Leonhardt
 */
public interface IDUUIUrlAccessible {
    /**
     * Returns the URL of the DUUI-component.
     *
     * @return
     */
    String generateURL();

    /**
     * Returns the connection handler for accessing connectors.
     *
     * @return
     */
    IDUUIConnectionHandler getHandler();

    /**
     * Returns the communication layer.
     *
     * @return
     * @see org.luaj.vm2.Lua
     */
    IDUUICommunicationLayer getCommunicationLayer();
}
