package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation;

import java.net.UnknownHostException;

public interface IDUUIMonitor {
    
    IDUUIMonitor setup() throws Exception; 

    default void shutdown() {};

    default Object generateURL() throws UnknownHostException {
        return null;
    }
}