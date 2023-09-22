package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import com.dropbox.core.*;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DownloadZipResult;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;

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
    private final DbxRequestConfig config;
    private final DbxClientV2 client;


    public DUUIDropboxDataReader(String appName) {
        config = DbxRequestConfig.newBuilder(appName).build();
        client = new DbxClientV2(config, ACCESS_TOKEN);
    }

    public DUUIDropboxDataReader(String appName, String userAccessToken) {
        config = DbxRequestConfig.newBuilder(appName).build();
        client = new DbxClientV2(config, userAccessToken);
    }

    @Override
    public void writeFile(ByteArrayInputStream source, String fileName, String target) {
        try {

            if (!target.startsWith("/") && !target.isEmpty()) {
                target = "/" + target;
            }

            if (!target.endsWith("/")) {
                target += "/";
            }

            client.files().uploadBuilder(target + fileName).uploadAndFinish(source);
        } catch (DbxException | IOException e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
        }
    }

    @Override
    public void writeFiles(List<ByteArrayInputStream> source, List<String> fileNames, String target) {
        for (int i = 0; i < source.size(); i++) {
            writeFile(source.get(i), fileNames.get(i), target);
        }
    }

    public ByteArrayInputStream readFile(String dbxSource) {
        ByteArrayOutputStream fileContentOutput = new ByteArrayOutputStream();
        try {
            DbxDownloader<FileMetadata> downloader = client.files().download(dbxSource);
            System.out.println(downloader.getResult().getSize());
            downloader.download(fileContentOutput);
            fileContentOutput.close();
            return new ByteArrayInputStream(fileContentOutput.toByteArray());
        } catch (DbxException | IOException e) {
            return null;
        }
    }

    public List<ByteArrayInputStream> readFiles(String dbxSource, String fileExtension) throws IOException {
        readProgress.set(0);
        List<String> files = listFiles(dbxSource, fileExtension);
        if (files.isEmpty()) {
            return new ArrayList<>();
        }

        List<ByteArrayInputStream> inputStreams = new ArrayList<>();

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
