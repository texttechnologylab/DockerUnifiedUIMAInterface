package org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport;

import java.io.IOException;
import java.io.InputStream;

public interface IDUUITransport {
    InputStream load() throws IOException;
}
