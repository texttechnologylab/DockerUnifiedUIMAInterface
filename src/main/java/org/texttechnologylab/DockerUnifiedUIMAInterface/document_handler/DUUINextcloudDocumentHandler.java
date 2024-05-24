package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import org.aarboard.nextcloud.api.NextcloudConnector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.aarboard.nextcloud.api.webdav.ResourceProperties;
import org.javatuples.Pair;

public class DUUINextcloudDocumentHandler implements IDUUIDocumentHandler {

    private NextcloudConnector connector;
    private String loginName;
    private String tempPath;

    /**
     * Create a new NextCloudDocumentHandler
     *
     * @param serverName      A URL containing the server address. (e.g.: "https://nextcloud.texttechnologylab.org/")
     * @param loginName       The username used to log in to a NextCloud account.
     * @param password       The password for the user.
     */
    public DUUINextcloudDocumentHandler(String serverName, String loginName, String password)  {
        connector = new NextcloudConnector(serverName, loginName, password);
        this.loginName = loginName;
        tempPath = System.getProperty("java.io.tmpdir") + "/";
    }

    public static void main(String[] args) throws IOException {

//        String serverName = "https://nextcloud.texttechnologylab.org/";
//        String loginName = "[USERNAME]";
//        String password = "[PASSWORD]";

//        DUUINextcloudDocumentHandler handler =
//                new DUUINextcloudDocumentHandler(serverName, loginName, password);

//        System.out.println(handler.connector.getCurrentUser().getDisplayname());
//        handler.connector.downloadFile("/apps/files/Readme.md", "/");

//        List<DUUIDocument> docs = handler.listDocuments("/", "", false)
//            .stream()
//            .map(doc -> {
//                try {
//                    return handler.readDocument(doc.getPath());
//                } catch (IOException e) {
//                    System.err.println("File download failed: " + doc.getPath());
//                    return null;
//                }
//            })
//            .filter(Predicate.not(Predicate.isEqual(null)))
//            .collect(Collectors.toList());

//        DUUIDocument doc = new DUUIDocument("3713534.xmi", "/home/stud_homes/s0424382/projects/DockerUnifiedUIMAInterface/test_corpora_xmi/3713534.xmi");
//        doc.setBytes(Files.readAllBytes(new File(doc.getPath()).toPath()));
//
//        handler.writeDocument(doc, "/");
//        DUUIDocument doc = handler.readDocument("ded.txt");
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        out.writeBytes(doc.getBytes());
//        System.out.println(out);
//
//        handler.connector.shutdown();
    }

    /**
     * Removes part of the path.
     * e.g.: From "remote.php/[username]/files/document.txt" to "document.txt"
     * Required for reading and writing files, since the full path causes an error.
     *
     * @param path      A path for a file on a Nextcloud server.
     */
    private String removeWebDavFromPath(String path) {
        int substringIndex = path.indexOf(loginName);
        if (substringIndex == -1) {
            return path; // Substring not found, return original string
        }
        return path.substring(substringIndex + loginName.length());

    }

    /**
     * All paths in Nextcloud read, write and list requests should end with a forward slash character.
     * The only exception are absolute paths ending in a file name.
     *
     * @param path The path to possibly modify
     * @return A (possibly) new path with a leading forward slash.
     */
    private String addTrailingSlashToPath(String path) {
        return !path.isEmpty() && !path.endsWith("/") ? path + "/" : path;
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
        if (!connector.folderExists(path)) {
            connector.createFolder(path);
        }

        path = addTrailingSlashToPath(path);

        connector.uploadFile(document.toInputStream(), path + document.getName());
    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) throws IOException {
        for (DUUIDocument document : documents) {
            writeDocument(document, path);
        }
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {

        DUUIDocument document = new DUUIDocument(Paths.get(path).getFileName().toString(), path);

        ResourceProperties metaData = connector.getProperties(path, true);
        document.setName(metaData.getDisplayName());
        document.setSize(metaData.getSize());

        connector.downloadFile(path, tempPath);
        File f = new File(tempPath + metaData.getDisplayName());
        document.setBytes(Files.readAllBytes(f.toPath()));
        f.delete();

        return document;
    }

    @Override
    public List<DUUIDocument> readDocuments(List<String> paths) throws IOException {
        if (paths.isEmpty()) {
            return new ArrayList<>();
        }

        List<DUUIDocument> documents = new ArrayList<>();
        for (String path : paths) {
            documents.add(readDocument(path));
        }
        return documents;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {

        return connector.listFolderContent(path, recursive ? -1 : 1, true, true)
                .stream()
                .map(this::removeWebDavFromPath)
                .map(fileName -> {
                    try {
                        return Pair.with(fileName, connector.getProperties(fileName, true));
                    } catch (IOException e) {
                        System.err.println("File not found: " + fileName);
                        return null;
                    }
                })
                .filter(Predicate.not(Predicate.isEqual(null)))
                .filter( metadata ->
                        metadata.getValue1().getDisplayName().endsWith(fileExtension) || fileExtension.isEmpty()
                )
                .map(metadata ->
                        new DUUIDocument(
                                metadata.getValue1().getDisplayName(),
                                metadata.getValue0(),
                                metadata.getSize())
                ).collect(Collectors.toList());

    }
}
