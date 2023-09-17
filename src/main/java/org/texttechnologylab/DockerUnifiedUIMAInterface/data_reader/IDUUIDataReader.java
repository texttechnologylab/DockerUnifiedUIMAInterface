package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

public interface IDUUIDataReader {
    void downloadFile(String sourceFile, String targetFolder);

    void downloadFiles(String sourcesFolder, String targetFolder);

    void uploadFile(String sourceFile, String targetPath);

    void uploadFiles(String sourceFolder, String targetPath);

    void listFiles(String folderPath);
}
