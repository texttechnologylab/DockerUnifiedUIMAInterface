package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import com.dropbox.core.*;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DownloadZipResult;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;

import java.io.*;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class DUUIDropboxDataReader implements IDUUIDataReader {

    private static final String ACCESS_TOKEN = System.getenv("dropbox_token");
    private final DbxRequestConfig config;
    private final DbxClientV2 client;

    public DUUIDropboxDataReader(String appName)  {
        config = DbxRequestConfig.newBuilder(appName).build();
        client = new DbxClientV2(config, ACCESS_TOKEN);
//        client.refreshAccessToken();
    }

    public DUUIDropboxDataReader(String appName, String userAccessToken) {
        config = DbxRequestConfig.newBuilder(appName).build();
        client = new DbxClientV2(config, userAccessToken);
    }

    public DUUIDropboxDataReader(DbxRequestConfig requestConfig) {
        config = requestConfig;
        client = new DbxClientV2(config, ACCESS_TOKEN);
    }

    private ZipFile unzipDownloadResult(String zipFile) throws IOException {
        try (ZipFile zip = new ZipFile(zipFile)) {
            Enumeration<? extends ZipEntry> entries = zip.entries();

            String targetDirectory = zipFile.replace(".zip", "");
            File nestedOutputDirectory = new File(targetDirectory);

            if (!nestedOutputDirectory.exists()) {
                if (!nestedOutputDirectory.mkdirs()) {
                    throw new RuntimeException("Folder not present.");
                }
            }

            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.isDirectory()) {
                    System.out.println("Skipping directory.");
                    continue;
                }
                String outputFile = Paths.get(nestedOutputDirectory.getPath(), entry.getName()).toString();

                File parentDir = new File(outputFile).getParentFile();
                if (!parentDir.exists()) {
                    parentDir.mkdirs();
                }

                try (InputStream inputStream = zip.getInputStream(entry);
                     FileOutputStream outputStream = new FileOutputStream(outputFile)) {

                    byte[] buffer = new byte[1024];
                    int bytesRead;

                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
            }
            return zip;
        }
    }

//    @Override
//    public String generateOAuthURL() {
//        DbxPKCEWebAuth pkceWebAuth = new DbxPKCEWebAuth(config, new DbxAppInfo(key));
//        DbxWebAuth.Request webAuthRequest = DbxWebAuth.newRequestBuilder()
//                .withNoRedirect()
//                .withTokenAccessType(TokenAccessType.OFFLINE)
//                .build();
//        return pkceWebAuth.authorize(webAuthRequest);
//    }


    @Override
    public void downloadFile(String sourceFile, String targetFolder) {
        try {
            DbxDownloader<FileMetadata> downloader = client.files().download(sourceFile);
            FileOutputStream out = new FileOutputStream(targetFolder);
            downloader.download(out);
            out.close();
        } catch (DbxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void downloadFiles(String sourcesFolder, String targetFolder) {

        if (!Paths.get(targetFolder).getFileName().toString().endsWith(".zip")) {
            throw new IllegalArgumentException("targetFolder must be a zip archive.");
        }

        DbxDownloader<DownloadZipResult> downloader = null;
        try {
            downloader = client.files().downloadZip(sourcesFolder);
        } catch (DbxException e) {
            throw new RuntimeException(e);
        }
        try {
            FileOutputStream out = new FileOutputStream(targetFolder);
            downloader.download(out);
            out.close();

            ZipFile unzipDownloadResult = unzipDownloadResult(targetFolder);
            File zipFile = new File(unzipDownloadResult.getName());
            if (!zipFile.delete()) {
                throw new RuntimeException("Zip file still there...");
            }

        } catch (DbxException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uploadFile(String sourceFile, String targetPath) {
        try {
            targetPath = targetPath.replace("\\", "/");
            if (!targetPath.startsWith("/")) {
                targetPath = "/" + targetPath;
            }
            client.files().uploadBuilder(targetPath).uploadAndFinish(new FileInputStream(sourceFile));
        } catch (DbxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.out.println("Failed. File not found.");
        }
    }

    @Override
    public void uploadFiles(String sourceFolder, String targetPath) {
        File input = new File(sourceFolder);
        if (!input.isDirectory()) {
            throw new IllegalArgumentException("Input can only be a directory.");
        }

        for (File file : Objects.requireNonNull(input.listFiles())) {
            if (!file.isFile()) continue;
            uploadFile(Paths.get(sourceFolder, file.getName()).toString(), Paths.get(targetPath, file.getName()).toString());
        }
    }

    @Override
    public void listFiles(String folderPath) {
        ListFolderResult result = null;
        try {
            result = client.files().listFolder(folderPath);
        } catch (DbxException e) {
            throw new RuntimeException(e);
        }

        while (true) {
            for (Metadata metadata : result.getEntries()) {
                System.out.println(metadata.getPathLower());
            }

            if (!result.getHasMore()) {
                break;
            }

            try {
                result = client.files().listFolderContinue(result.getCursor());
            } catch (DbxException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
