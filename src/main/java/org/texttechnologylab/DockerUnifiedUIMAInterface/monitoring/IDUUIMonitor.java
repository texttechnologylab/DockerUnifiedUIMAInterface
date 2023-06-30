package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.net.UnknownHostException;

public interface IDUUIMonitor {
    
    IDUUIMonitor setup() throws Exception; 

    void shutdown();

    Object generateURL() throws UnknownHostException; 
}