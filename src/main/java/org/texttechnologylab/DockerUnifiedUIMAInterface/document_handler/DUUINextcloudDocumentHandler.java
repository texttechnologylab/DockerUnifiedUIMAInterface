package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.impl.SardineImpl;
import org.aarboard.nextcloud.api.AuthenticationConfig;
import org.aarboard.nextcloud.api.ServerConfig;
import org.aarboard.nextcloud.api.exception.NextcloudApiException;
import org.aarboard.nextcloud.api.provisioning.ProvisionConnector;
import org.aarboard.nextcloud.api.provisioning.User;
import org.aarboard.nextcloud.api.utils.ConnectorCommon;
import org.aarboard.nextcloud.api.utils.WebdavInputStream;
import org.aarboard.nextcloud.api.webdav.AWebdavHandler;
import org.aarboard.nextcloud.api.webdav.Folders;
import org.aarboard.nextcloud.api.webdav.pathresolver.NextcloudVersion;
import org.aarboard.nextcloud.api.webdav.pathresolver.WebDavPathResolver;
import org.aarboard.nextcloud.api.webdav.pathresolver.WebDavPathResolverBuilder;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;

import javax.xml.namespace.QName;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DUUINextcloudDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    class NCFolders extends Folders {

        private WebDavPathResolver _resolver;

        public NCFolders(ServerConfig serverConfig) {
            super(serverConfig);

            _resolver = WebDavPathResolverBuilder.get(WebDavPathResolverBuilder.TYPE.FILES)
                    .ofVersion(NextcloudVersion.get(getServerVersion()))
                    .withUserName(userId)
                    .withBasePathSuffix("files")
                    .withBasePathPrefix(_serverConfig.getSubPathPrefix()).build();
        }

        protected Sardine buildAuthSardine() {
            return buildAuthSardine0();
        }

        protected String buildWebdavPath(String remotePath) {
            return buildWebdavPath(_resolver, remotePath);
        }
    }

    class NCFiles extends org.aarboard.nextcloud.api.webdav.Files {

        private WebDavPathResolver _resolver;

        public NCFiles(ServerConfig serverConfig) {
            super(serverConfig);
            _resolver = WebDavPathResolverBuilder.get(WebDavPathResolverBuilder.TYPE.FILES)
                .ofVersion(NextcloudVersion.get(getServerVersion()))
                .withUserName(userId)
                .withBasePathSuffix("files")
                .withBasePathPrefix(_serverConfig.getSubPathPrefix()).build();
        }

        protected Sardine buildAuthSardine() {
            return buildAuthSardine0();
        }

        protected String buildWebdavPath(String remotePath) {
            return buildWebdavPath(_resolver, remotePath);
        }
    }

    class OpenSardine extends SardineImpl {


        private CredentialsProvider createDefaultCredentialsProvider(String username, String password, String domain, String workstation) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            if (username != null) {
                provider.setCredentials(
                        new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.NTLM),
                        new NTCredentials(username, password, workstation, domain));
                provider.setCredentials(
                        new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.BASIC),
                        new UsernamePasswordCredentials(username, password));
                provider.setCredentials(
                        new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.DIGEST),
                        new UsernamePasswordCredentials(username, password));
                provider.setCredentials(
                        new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                        new NTCredentials(username, password, workstation, domain));
                provider.setCredentials(
                        new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.KERBEROS),
                        new UsernamePasswordCredentials(username, password));
            }
            return provider;
        }

        private HttpClientBuilder createHttpClientBuilder(String username, String password) {
            return this.configure(null,
                this.createDefaultCredentialsProvider(username, password, null, null))
                    .disableCookieManagement();
        }

        public void addCookies(String username, String password) {
            this.client = createHttpClientBuilder(username, password).build();
        }

        public OpenSardine(String username, String password) {
            super(username, password);
            addCookies(username, password);
        }

        @Override
        public void shutdown() throws IOException {
            if (isShutdown.get()) super.shutdown();
        }
    }


    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final NCFolders folders;
    private final NCFiles files;
    private final String userId;
    private final String loginName;
    ServerConfig _serverConfig;
    Sardine _sardine;
    URL _serviceUrl;

    /**
     * Create a new NextCloudDocumentHandler
     *
     * @param serverName      A URL containing the server address. (e.g.: "https://nextcloud.texttechnologylab.org/")
     * @param loginName       The username used to log in to a NextCloud account.
     * @param password       The password for the user.
     */
    public DUUINextcloudDocumentHandler(String serverName, String loginName, String password) {
        this.loginName = loginName;

        try {

            _serviceUrl = new URL(serverName);
            _serverConfig = new ServerConfig(_serviceUrl.getHost(), true, _serviceUrl.getPort(),
                    new AuthenticationConfig(loginName, password));

            buildAuthSardine0();

            ProvisionConnector pc = new ProvisionConnector(_serverConfig);

            User user = pc.getCurrentUser();
            userId = user.getId();
            folders  = new NCFolders(_serverConfig);
            files    = new NCFiles(_serverConfig);
            System.out.printf("[DUUINextCloudConnector] Connected to NextCloud %s as %s%n",
                    files.getServerVersion(), user.getDisplayname());
        } catch (NextcloudApiException e) {
            Throwable t = e.getCause();
            if (t instanceof ExecutionException) t = t.getCause();

            String message = t.getMessage();
            if (message.contains("401")) {
                message = "Invalid username or password.";
            } else if (message.contains("404")) {
                message = "Unexpected response (404 Not Found): " + serverName;
            }

            throw new RuntimeException(message, t);
        } catch (MalformedURLException | IllegalArgumentException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) return;

        try {
            _sardine.shutdown();
            ConnectorCommon.shutdown();
        } catch (IOException e) { throw new RuntimeException(e); }
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



    Sardine buildAuthSardine0() {
        if (_sardine != null) return _sardine;

        Sardine sardine = new OpenSardine(
                _serverConfig.getUserName(),
                _serverConfig.getAuthenticationConfig().getPassword()
        );
        sardine.ignoreCookies();
        sardine.enablePreemptiveAuthentication(_serverConfig.getServerName());

        _sardine = sardine;
        return _sardine;
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

        if (substringIndex != -1) {
            return path.substring(substringIndex + loginName.length());
        } else if (path.contains("/remote.php/webdav/")) {
            return path.replace("/remote.php/webdav/", "");
        }

        return path;
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

        document.setStatus(DUUIStatus.OUTPUT);
        synchronized (path) {
            if (!folders.exists(path)) {
                folders.createFolder(path);
            }
        }

        path = addTrailingSlashToPath(path);

        document.setUploadProgress(0);
        try {
            files.uploadFile(new ByteArrayInputStream(document.toInputStream().readAllBytes()), path + document.getName());
        } catch (Exception e) {
            throw new RuntimeException("Uploading following file failed: " + path + document.getName(), e);
        }

        document.setUploadProgress(100);
    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) {
        Set<String> seenNames = new HashSet<>();
        documents.stream()
            .filter(doc -> seenNames.add(doc.getName()))
            .parallel().forEach(d -> writeDocument(d, path));
    }

    @Override
    public DUUIDocument readDocument(String path) {
        String displayName = Paths.get(path).getFileName().toString();
        DUUIDocument document = new DUUIDocument(displayName, path);

        try (InputStream df = files.downloadFile(path)) {
            byte[] bytes = df.readAllBytes();
            document.setBytes(bytes);
            document.setSize(bytes.length);
        } catch (Exception e) {
            throw new RuntimeException("Downloading following file failed: " + path, e);
        }

        return document;
    }

    @Override
    public List<DUUIDocument> readDocuments(List<String> paths) throws IOException {
        return paths.parallelStream()
                .map(this::readDocument)
                .collect(Collectors.toList());
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) {

        try {
            return CompletableFuture.supplyAsync(() -> listFolderContent(path, recursive ? -1 : 1)
                .stream()
                .filter(res -> !res.isDirectory())
                .filter( res -> fileExtension.isEmpty() || res.getPath().endsWith(fileExtension))
                .map(res -> {
                    String fsize = res.getCustomProps().get("size");
                    long size = fsize != null && !fsize.isEmpty() ? Long.parseLong(fsize) : 0;
                    String fpath = removeWebDavFromPath(res.getPath());

                    return new DUUIDocument(res.getDisplayName(), fpath, size);
                })
                .collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

    }

    public List<DavResource> listFolderContent(String remotePath, int depth) {
        String pathPrefix = AWebdavHandler.WEB_DAV_BASE_PATH;
        String path = new URIBuilder()
            .setScheme("https")
            .setHost(_serverConfig.getServerName())
            .setPort(_serverConfig.getPort())
            .setPath(pathPrefix + remotePath).toString();

        List<DavResource> resources;
        try {
            Set<QName> props= new HashSet<>();
            props.add(new QName("DAV:", "displayname", "d"));
            props.add(new QName("http://owncloud.org/ns", "size", "oc"));

//            System.out.println("Searching for folder " + path);
//            long startTime = System.currentTimeMillis();
            resources = buildAuthSardine0().list(path, depth, props);
//            startTime = System.currentTimeMillis() - startTime;
//            System.out.println("Found " + resources.size() + " folders in " + startTime + "ms");
        } catch (IOException e) {
            throw new NextcloudApiException(e);
        }

        return resources;
    }

    @Override
    public DUUIFolder getFolderStructure() {

        DUUIFolder root = new DUUIFolder("/", "Files");

        Map<String, DUUIFolder> parentMap = new HashMap<>();
        parentMap.put("/", root);

        try {
            CompletableFuture.supplyAsync(() -> listFolderContent("/", -1)
                .stream()
                .filter(DavResource::isDirectory)
                .map(res -> {
                    String f = "/" + removeWebDavFromPath(res.getPath());

                    if (f.equals("/")) return 0;

                    String[] splitPath = f.split("/");
                    String folderName = res.getDisplayName();
                    DUUIFolder folder = new DUUIFolder(f, folderName);
                    String parent = splitPath.length > 2 ? splitPath[splitPath.length - 2] : "/";

                    parentMap.put(folderName, folder);
                    parentMap.get(parent).addChild(folder);

                    return 1;
                }).toList()
            ).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return root;
    }
}
