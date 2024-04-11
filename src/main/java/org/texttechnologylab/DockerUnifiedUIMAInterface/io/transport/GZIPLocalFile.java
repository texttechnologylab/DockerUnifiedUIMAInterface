package org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;

public class GZIPLocalFile implements IDUUITransport {
    private final Path path;

    public GZIPLocalFile(Path path) {
        this.path = path;
    }

    @Override
    public InputStream load() throws IOException {
        return new GZIPInputStream(Files.newInputStream(path, StandardOpenOption.READ));
    }
}
