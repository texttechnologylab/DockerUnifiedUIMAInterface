package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class DUUILocalDataReader implements IDUUIDataReader {
    @Override
    public void writeFile(DUUIExternalFile source, String fileName, String target) {

    }

    @Override
    public void writeFiles(List<DUUIExternalFile> source, List<String> fileNames, String target) {

    }

    @Override
    public DUUIExternalFile readFile(String source) {
        return null;
    }

    @Override
    public List<DUUIExternalFile> readFiles(String source, String fileExtension) throws IOException {
        return null;
    }

    @Override
    public List<String> listFiles(String folderPath) throws IOException {

        return null;
    }

    @Override
    public List<String> listFiles(String folderPath, String fileExtension) throws IOException {
        return null;
    }
}
