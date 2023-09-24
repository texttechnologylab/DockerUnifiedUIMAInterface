package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface IDUUIDataReader {

    AtomicInteger readProgress = new AtomicInteger(0);

    void writeFile(DUUIExternalFile source, String fileName, String target);

    void writeFiles(List<DUUIExternalFile> source, List<String> fileNames, String target);

    DUUIExternalFile readFile(String source);

    List<DUUIExternalFile> readFiles(String source, String fileExtension) throws IOException;


    List<String> listFiles(String folderPath) throws IOException;

    List<String> listFiles(String folderPath, String fileExtension) throws IOException;

}
