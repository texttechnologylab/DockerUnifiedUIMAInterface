package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import com.dropbox.core.*;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.sharing.ListFoldersBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DUUIDropboxDataReader implements IDUUIDataReader {

    private static final String ACCESS_TOKEN = System.getenv("dbx_personal_access_token");
    private static final String REFRESH_TOKEN = System.getenv("dbx_personal_refresh_token");
    private static final String APP_KEY = System.getenv("dbx_app_key");
    private static final String APP_SECRET = System.getenv("dbx_app_secret");
    private final DbxRequestConfig config;
    private final DbxClientV2 client;

    public DUUIDropboxDataReader(String appName) {
        config = DbxRequestConfig.newBuilder(appName).build();
        DbxCredential credentials = new DbxCredential(ACCESS_TOKEN, 1L, REFRESH_TOKEN, APP_KEY, APP_SECRET);
        client = new DbxClientV2(config, credentials);
        try {
            client.refreshAccessToken();
        } catch (DbxException e) {
            throw new RuntimeException(e);
        }
    }

    public DUUIDropboxDataReader(String appName, String accessToken, String refreshToken) {
        config = DbxRequestConfig.newBuilder(appName).build();
        DbxCredential credentials = new DbxCredential(accessToken, 1L, refreshToken, APP_KEY, APP_SECRET);
        client = new DbxClientV2(config, credentials);
        try {
            client.refreshAccessToken();
        } catch (DbxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeFile(DUUIInputStream stream, String target) {
        try {

            if (!target.startsWith("/") && !target.isEmpty()) {
                target = "/" + target;
            }

            if (!target.endsWith("/")) {
                target += "/";
            }

            client.files().uploadBuilder(target + stream.getName()).uploadAndFinish(stream.getContent());
        } catch (DbxException | IOException e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
        }
    }

    @Override
    public void writeFiles(List<DUUIInputStream> streams, String target) {
        for (DUUIInputStream duuiExternalFile : streams) {
            writeFile(duuiExternalFile, target);
        }
    }

    public DUUIInputStream readFile(String dbxSource) {
        ByteArrayOutputStream fileContentOutput = new ByteArrayOutputStream();
        try {
            DbxDownloader<FileMetadata> downloader = client.files().download(dbxSource);
            downloader.download(fileContentOutput);
            fileContentOutput.close();
            FileMetadata metadata = downloader.getResult();
            return new DUUIInputStream(
                metadata.getName(),
                metadata.getPathLower().replace("/" + metadata.getName(), ""),
                metadata.getSize(),
                new ByteArrayInputStream(fileContentOutput.toByteArray())
            );
        } catch (DbxException | IOException e) {
            return null;
        }
    }


    public List<DUUIInputStream> readFiles(List<String> paths) {
        readProgress.set(0);

        if (paths.isEmpty()) {
            return new ArrayList<>();
        }

        List<DUUIInputStream> inputStreams = new ArrayList<>();

        for (String file : paths) {
            inputStreams.add(readFile(file));
        }
        return inputStreams;
    }

    @Override
    public List<String> listFiles(String folderPath, String fileExtension, boolean recursive) throws IOException {
        ListFolderResult result = null;

        try {
            result = client.files().listFolderBuilder(folderPath).withRecursive(recursive).start();
        } catch (DbxException e) {
            System.out.println(e.getMessage());
            throw new IOException("Dropbox could not get more files.");
        }

        List<String> paths = new ArrayList<>();

        for (Metadata metadata : result.getEntries()) {
            if (metadata.getPathLower().endsWith(fileExtension)) {
                paths.add(metadata.getPathLower());
            }
        }

        return paths;
    }
}
