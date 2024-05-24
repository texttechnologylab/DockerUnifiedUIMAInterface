package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.User;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class DUUIGoogleDriveDocumentHandler implements IDUUIDocumentHandler{

    private static final String APPLICATION_NAME = "DriveDocumentHandler";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";

    /**
     * Global instance of the scopes required by this quickstart.
     * If modifying these scopes, delete your previously saved tokens/ folder.
     */
    private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE_METADATA_READONLY);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";

    private Drive service;

    public DUUIGoogleDriveDocumentHandler() throws GeneralSecurityException, IOException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }


    /**
     * Creates an authorized Credential object.
     * @param HTTP_TRANSPORT The network HTTP Transport.
     * @return An authorized Credential object.
     * @throws IOException If the credentials.json file cannot be found.
     */
    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = DUUIGoogleDriveDocumentHandler.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    public static void main(String... args) throws IOException, GeneralSecurityException {
        // Build a new authorized API client service.
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();

        // Print the names and IDs for up to 10 files.


        FileList result = service.files().list()
                .setOrderBy("folder, modifiedTime, name")
                .setQ("parents In '1KqAYa-YLPBSRpv1y7LTYS0OV115CphiD'")
//                .setPageSize(10)
                .setFields("files(parents, id, name)")
                .execute();
        List<File> files = result.getFiles();
        if (files == null || files.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (File file : files) {
//                System.out.printf("%s (%s)\n", file.getName(), file.getId());

                System.out.printf("%s (%s)\n", file.getName(), file.getParents() == null ? " " : String.join(", ", file.getParents()));
            }

        }
    }



    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) throws IOException {

    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
//        File file = service.files().get(path).executeMediaAsInputStream().execute();
//        file.get
//        return new DUUIDocument(file.getId(), file.getParents().get(0), file.getSize());
        return null;
    }

    @Override
    public List<DUUIDocument> readDocuments(List<String> paths) throws IOException {
        return List.of();
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

        FileList result = service.files().list()
                .setQ(searchPath + " mimeType != 'application/vnd.google-apps.folder' "
                        + String.format("fileExtension = '%s'", fileExtension))
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
}
