package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface IDUUIDataReader {

    AtomicInteger readProgress = new AtomicInteger(0);

    void writeFile(ByteArrayInputStream source, String fileName, String target);

    void writeFiles(List<ByteArrayInputStream> source, List<String> fileNames, String target);

    ByteArrayInputStream readFile(String source);

    List<ByteArrayInputStream> readFiles(String source, String fileExtension) throws IOException;

    List<String> listFiles(String folderPath) throws IOException;

    List<String> listFiles(String folderPath, String fileExtension) throws IOException;


}
