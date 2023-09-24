package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import com.dropbox.core.*;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DownloadZipResult;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.REF;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class DUUIDropboxDataReader implements IDUUIDataReader {

    private static final String ACCESS_TOKEN = System.getenv("dbx_personal_access_token");
    private static final String REFRESH_TOKEN = System.getenv("dbx_personal_refresh_token");
    private static final String APP_KEY = System.getenv("dbx_app_key");
    private static final String APP_SECRET = System.getenv("dbx_app_secret");
    private final DbxRequestConfig config;
    private final DbxClientV2 client;

    private void authorize() throws IOException {

        DbxPKCEWebAuth pkceWebAuth = new DbxPKCEWebAuth(config, new DbxAppInfo(APP_KEY));
        DbxWebAuth.Request webAuthRequest = DbxWebAuth.newRequestBuilder()
                .withNoRedirect()
                .withTokenAccessType(TokenAccessType.OFFLINE)
                .build();
    }

    public DUUIDropboxDataReader(String appName) {
        config = DbxRequestConfig.newBuilder(appName).build();
        DbxCredential credentials = new DbxCredential(ACCESS_TOKEN, 1L, REFRESH_TOKEN, APP_KEY, APP_SECRET);
        System.out.println(REFRESH_TOKEN);
        client = new DbxClientV2(config, credentials);
        try {
            client.refreshAccessToken();
        } catch (DbxException e) {
            throw new RuntimeException(e);
        }
    }

    public DUUIDropboxDataReader(String appName, String userAccessToken) {
        config = DbxRequestConfig.newBuilder(appName).build();
        client = new DbxClientV2(config, userAccessToken);
    }

    @Override
    public void writeFile(DUUIExternalFile source, String fileName, String target) {
        try {

            if (!target.startsWith("/") && !target.isEmpty()) {
                target = "/" + target;
            }

            if (!target.endsWith("/")) {
                target += "/";
            }

            client.files().uploadBuilder(target + fileName).uploadAndFinish(source.getContent());
        } catch (DbxException | IOException e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
        }
    }

    @Override
    public void writeFiles(List<DUUIExternalFile> source, List<String> fileNames, String target) {
        for (int i = 0; i < source.size(); i++) {
            writeFile(source.get(i), fileNames.get(i), target);
        }
    }

    public DUUIExternalFile readFile(String dbxSource) {
        ByteArrayOutputStream fileContentOutput = new ByteArrayOutputStream();
        try {
            DbxDownloader<FileMetadata> downloader = client.files().download(dbxSource);
            downloader.download(fileContentOutput);
            fileContentOutput.close();
            FileMetadata metadata = downloader.getResult();
            return new DUUIExternalFile(
                    metadata.getName(),
                    metadata.getPathLower(),
                    metadata.getSize(),
                    new ByteArrayInputStream(fileContentOutput.toByteArray())
            );
        } catch (DbxException | IOException e) {
            return null;
        }
    }

    public List<DUUIExternalFile> readFiles(String dbxSource, String fileExtension) throws IOException {
        readProgress.set(0);
        List<String> files = listFiles(dbxSource, fileExtension);
        if (files.isEmpty()) {
            return new ArrayList<>();
        }

        List<DUUIExternalFile> inputStreams = new ArrayList<>();

        for (String file : files) {
            int progress = Math.round((float) readProgress.incrementAndGet() / files.size() * 100);
            System.out.println("Progress " + progress + " % \r");
            if (file.endsWith(fileExtension)) {
                inputStreams.add(readFile(file));
            }
        }
        return inputStreams;
    }


    @Override
    public List<String> listFiles(String folderPath) throws IOException {
        return listFiles(folderPath, "");
    }

    @Override
    public List<String> listFiles(String folderPath, String fileExtension) throws IOException {
        ListFolderResult result = null;

        try {
            result = client.files().listFolder(folderPath);
        } catch (DbxException e) {
            System.out.println(e.getMessage());
            throw new IOException("Dropbox could not get more files.");
        }

        List<String> files = new ArrayList<>();

        while (true) {
            for (Metadata metadata : result.getEntries()) {
                if (metadata.getPathLower().endsWith(fileExtension)) {
                    files.add(metadata.getPathLower());
                }
            }

            if (!result.getHasMore()) {
                break;
            }

            try {
                result = client.files().listFolderContinue(result.getCursor());
            } catch (DbxException e) {
                System.out.println(e.getMessage());
            }
        }

        return files;
    }
}
