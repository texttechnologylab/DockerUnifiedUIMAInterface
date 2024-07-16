package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class DUUIGoogleDriveDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    private static final String APPLICATION_NAME = "DUUI Controller";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private final Drive service;
    public DUUIGoogleDriveDocumentHandler(Credential credential) throws GeneralSecurityException, IOException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    public static void main(String... args) throws IOException, GeneralSecurityException {

//        DUUIGoogleDriveDocumentHandler handler = new DUUIGoogleDriveDocumentHandler();
//        DUUIDocument doc = handler.readDocument(handler.getFileId("firstpdf.pdf"));

//
//        doc.setName("secondpdf.pdf");
//
//        handler.writeDocument(doc, handler.getFolderId("first"));

//        System.out.println(handler.getFolderStructure().toJson().toString());

    }



    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {

        File file = new File();
        file.setParents(Collections.singletonList(path));
        file.setName(document.getName());


        service.files().create(file, new InputStreamContent(null, document.toInputStream()))
            .execute();
    }


    @Override
    public DUUIDocument readDocument(String path) throws IOException {

        File file = service.files().get(path).execute();

        DUUIDocument document = new DUUIDocument(file.getName(), path);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        service.files().get(path).executeMediaAndDownloadTo(out);

        document.setBytes(out.toByteArray());

        return document;
    }

    private String getFolderId(String folderName) {

        FileList result = null;

        try {
            result = service.files().list()
                    .setQ(String.format("name = '%s' and mimeType = 'application/vnd.google-apps.folder'", folderName))
                    .setFields("files(parents, id, name)")
                    .execute();

        } catch (IOException e) {
            return "";
        }

        List<File> files = result.getFiles();

        if (files.isEmpty()) return "";

        return files.get(0).getId();
    }

    private String getFileId(String fileName) {

        FileList result = null;

        try {
            result = service.files().list()
                    .setQ(String.format("name = '%s'", fileName))
                    .setFields("files(parents, id, name)")
                    .execute();

        } catch (IOException e) {
            return "";
        }

        List<File> files = result.getFiles();

        if (files.isEmpty()) return "";

        return files.get(0).getId();
    }

    private String getAllSubFolders(String parent)  {

        FileList result = null;
        try {
            result = service.files().list()
                    .setQ(String.format("'%s' in parents", parent) + " and mimeType = 'application/vnd.google-apps.folder'")
                    .setFields("files(parents, id, name)")
                    .execute();
        } catch (IOException e) {
            return String.format("'%s' in parents ", parent);
        }

        List<File> files =  result.getFiles();

        String subfolders = files.stream()
                .map(File::getId)
                .map(this::getAllSubFolders)
                .collect(Collectors.joining(" or "));

        String addOn = !files.isEmpty() ? " or " + subfolders : "";

        return String.format("'%s' in parents", parent) + addOn;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {

        String searchPath = recursive ? getAllSubFolders(path) : String.format("'%s' in parents ", path);

        return listDocuments_(searchPath, fileExtension);
    }

    @Override
    public List<DUUIDocument> listDocuments(List<String> paths, String fileExtension, boolean recursive) throws IOException {

        String searchPath = paths.stream()
                .map(path -> String.format("'%s' in parents ", path))
                .collect(Collectors.joining(" or "));

        return listDocuments_(searchPath, fileExtension);
    }

    public List<DUUIDocument> listDocuments_(String searchPath, String fileExtension) throws IOException {

        String fileExtension_ = fileExtension.isEmpty() ? "" : String.format("fileExtension = '%s'", fileExtension);
        FileList result = service.files().list()
                .setQ(searchPath + " mimeType != 'application/vnd.google-apps.folder' "
                        + fileExtension_)
                .setFields("files(parents, id, name, size)")
                .execute();

        List<File> files =  result.getFiles();

        List<DUUIDocument> documents;

        if (files == null || files.size() != 1) {
            documents = List.of();
        } else {
            documents = files.stream()
                    .map(f -> new DUUIDocument(f.getId(), f.getParents().get(0), f.getSize()))
                    .collect(Collectors.toList());
        }

        return documents;
    }

    @Override
    public DUUIFolder getFolderStructure() {

        DUUIFolder root = new DUUIFolder("root", "Files");

        return getFolderStructure(root);
    }

    public DUUIFolder getFolderStructure(DUUIFolder root) {

        FileList result = null;
        try {
            result = service.files().list()
                    .setQ(String.format("'%s' in parents", root.id) + " and mimeType = 'application/vnd.google-apps.folder'")
                    .setFields("files(parents, id, name)")
                    .execute();
        } catch (IOException e) {
            return root;
        }

        List<File> files =  result.getFiles();

        for (File file : files) {
            DUUIFolder f = new DUUIFolder(file.getId(), file.getName());
            getFolderStructure(f);
            root.addChild(f);
        }

        return root;
    }
}
