package org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LocalFile implements IDUUITransport {
    private final Path path;

    public LocalFile(Path path) {
        this.path = path;
    }

    @Override
    public InputStream load() throws IOException {
        return Files.newInputStream(path, StandardOpenOption.READ);
    }
}
