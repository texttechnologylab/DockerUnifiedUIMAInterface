package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import com.github.sardine.impl.SardineImpl;
import org.aarboard.nextcloud.api.AuthenticationConfig;
import org.aarboard.nextcloud.api.NextcloudConnector;
import org.aarboard.nextcloud.api.ServerConfig;
import org.javatuples.Pair;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DUUINextcloudDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    private final NextcloudConnector connector;
    private final String loginName;
    private final String tempPath;
    ServerConfig _serverConfig;
    Sardine _sardine;

    /**
     * Create a new NextCloudDocumentHandler
     *
     * @param serverName      A URL containing the server address. (e.g.: "https://nextcloud.texttechnologylab.org/")
     * @param loginName       The username used to log in to a NextCloud account.
     * @param password       The password for the user.
     */
    public DUUINextcloudDocumentHandler(String serverName, String loginName, String password) {
        connector = new NextcloudConnector(serverName, loginName, password);
        this.loginName = loginName;
        tempPath = "." + System.getProperty("java.io.tmpdir") + "/";
        URL _serviceUrl;
        try {
            _serviceUrl = new URL(serverName);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        _serverConfig = new ServerConfig(_serviceUrl.getHost(), true, _serviceUrl.getPort(),
                new AuthenticationConfig(loginName, password));
//        Folders folders = new Folders(_serverConfig);
        _sardine = buildAuthSardine();

    }


    public static void main(String[] args) throws IOException {

//        String serverName = "https://nextcloud.texttechnologylab.org/";
//        String loginName = [USERNAME];
//        String password = [PASSWORD];
//
//        DUUINextcloudDocumentHandler handler =
//                new DUUINextcloudDocumentHandler(serverName, loginName, password);
//
//
//
//        System.out.println(handler.getFolderStructure().toJson());
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

    Sardine buildAuthSardine()
    {
        if (_serverConfig.getAuthenticationConfig().usesBasicAuthentication()) {
            Sardine sardine = SardineFactory.begin();
            sardine.setCredentials(_serverConfig.getUserName(),
                    _serverConfig.getAuthenticationConfig().getPassword());
            sardine.enablePreemptiveAuthentication(_serverConfig.getServerName());
            return sardine;
        }
        return new SardineImpl(_serverConfig.getAuthenticationConfig().getBearerToken());
    }

    private boolean isFolder(String path) {
        return path.endsWith("/");
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
    public void writeDocument(DUUIDocument document, String path) {
        if (!connector.folderExists(path)) {
            connector.createFolder(path);
        }

        path = addTrailingSlashToPath(path);

        document.setUploadProgress(0);
        connector.uploadFile(document.toInputStream(), path + document.getName());
        document.setUploadProgress(100);
    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) {
        for (DUUIDocument document : documents) {
            writeDocument(document, path);
        }
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        String displayName = Paths.get(path).getFileName().toString();
        DUUIDocument document = new DUUIDocument(displayName, path);

        if (connector.downloadFile(path, tempPath)) {
            byte[] bytes = Files.readAllBytes(new File(tempPath + displayName).toPath());
            document.setBytes(bytes);
            document.setSize(bytes.length);
        }

        return document;
    }

    @Override
    public List<DUUIDocument> readDocuments(List<String> paths) throws IOException {
        List<DUUIDocument> documents = new ArrayList<>();
        for (String path : paths) {
            documents.add(readDocument(path));
        }
        return documents;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) {

        try {
            return CompletableFuture.supplyAsync(() -> connector.listFolderContent(path, recursive ? -1 : 1, true, true)
                    .stream()
                    .filter( f -> f.endsWith(fileExtension) || fileExtension.isEmpty())
                    .map(this::removeWebDavFromPath)
                    .map(fileName -> {
                        try {
                            return Pair.with(fileName, connector.getProperties(fileName, true));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(Predicate.not(Predicate.isEqual(null)))

                    .map(metadata ->
                            new DUUIDocument(
                                    metadata.getValue1().getDisplayName(),
                                    metadata.getValue0(),
                                    metadata.getSize())
                    ).collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public DUUIFolder getFolderStructure() {

        DUUIFolder root = new DUUIFolder("/", "Files");

        Map<String, DUUIFolder> parentMap = new HashMap<>();
        parentMap.put("/", root);
        connector.listFolderContent("/", -1, false, true)
            .stream()
            .map(this::removeWebDavFromPath)
            .map(f -> f.startsWith("/") ? f: "/" + f)
            .filter(this::isFolder)
            .forEach(f -> {
                if (f.equals("/")) return;

                String[] splitPath = f.split("/");
                String folderName = this.getFolderName(f);
                DUUIFolder folder = new DUUIFolder(f, folderName);
                String parent = splitPath.length > 2 ? splitPath[splitPath.length - 2] : "/";

                parentMap.put(folderName, folder);
                parentMap.get(parent).addChild(folder);
            });

        return root;
    }

    private String getFolderName(String f) {

        if (f.length() <= 1) return f;
        String[] split = f.substring(0, f.length() - 1).split("/");
        return split[split.length - 1];

    }
}
