package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.IOException;
import java.util.List;

public class DUUILocalDataReader implements IDUUIDataReader {
    @Override
    public void writeFile(DUUIInputStream stream, String target) {

    }

    @Override
    public void writeFiles(List<DUUIInputStream> streams, String target) {

    }

    @Override
    public DUUIInputStream readFile(String source) {
        return null;
    }

    @Override
    public List<DUUIInputStream> readFiles(List<String> paths) throws IOException {
        return null;
    }

    @Override
    public List<String> listFiles(String folderPath) throws IOException {
        return null;
    }

    @Override
    public List<String> listFiles(String folder, String fileExtension, boolean recursive) throws IOException {
        return null;
    }

    @Override
    public List<String> listFiles(String folderPath, String fileExtension) throws IOException {
        return null;
    }
}
