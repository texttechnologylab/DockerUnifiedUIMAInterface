package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;

public interface IDUUIUrlAccessible {
    public String generateURL();
    public IDUUICommunicationLayer getCommunicationLayer();
}
