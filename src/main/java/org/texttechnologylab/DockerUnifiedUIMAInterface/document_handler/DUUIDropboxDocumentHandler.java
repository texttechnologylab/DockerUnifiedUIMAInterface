package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.dropbox.core.*;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.util.IOUtil;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public class DUUIDropboxDocumentHandler implements IDUUIDocumentHandler, IDUUIFolderPickerApi {

    private static final long CHUNK_SIZE = 8L << 20; // 8MiB
    private static final long MAX_RETRIES = 5;
    private final DbxClientV2 client;
    private WriteMode writeMode = WriteMode.ADD;
    private boolean debug;


    /**
     * Create a new DropboxDataReader
     *
     * @param config      A configuration object that contains the App Name.
     * @param credentials A credentials object that contains the access token, refresh token, app key and app secret.
     */
    public DUUIDropboxDocumentHandler(DbxRequestConfig config, DbxCredential credentials) throws DbxException {
        client = new DbxClientV2(config, credentials);
        client.refreshAccessToken();
    }

    /**
     * All paths in Dropbox read, write and list requests should start with a forward slash character.
     *
     * @param path The path to possibly modify
     * @return A (possibly) new path with a leading forward slash.
     */
    private String addLeadingSlashToPath(String path) {
        return !path.isEmpty() && !path.startsWith("/") ? "/" + path : path;
    }

    /**
     * All paths in Dropbox read, write and list requests should end with a forward slash character.
     * The only exception are absolute paths ending in a file name.
     *
     * @param path The path to possibly modify
     * @return A (possibly) new path with a leading forward slash.
     */
    private String addTrailingSlashToPath(String path) {
        return !path.isEmpty() && !path.endsWith("/") ? path + "/" : path;
    }

    /**
     * Gets the current writemode.
     *
     * @return The current writemode (ADD or OVERWRITE)
     */
    public WriteMode getWriteMode() {
        return writeMode;
    }

    /**
     * Sets the current writemode. If ADD is set, conflicting write operations throw an Exception.
     */
    public void setWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    /**
     * If debug is set to true, print upload and download progress to standard output.
     *
     * @return Current debug flag.
     */
    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
        try {
            path = addTrailingSlashToPath(addLeadingSlashToPath(path));
            document.setStatus(DUUIStatus.OUTPUT);
            IOUtil.ProgressListener progressListener = new IOUtil.ProgressListener() {
                long uploadProgress = 0;

                @Override
                public void onProgress(long progress) {
                    document.setUploadProgress(progress + uploadProgress);
                    if (progress == CHUNK_SIZE) uploadProgress += CHUNK_SIZE;
                    if (debug) System.out.printf("Bytes uploaded: %d", document.getUploadProgress());
                }
            };

            if (document.getSize() >= CHUNK_SIZE) {
                writeDocumentChunked(document, path + document.getName(), progressListener);
            } else {
                client.files()
                    .uploadBuilder(path + document.getName())
                    .withMode(writeMode)
                    .uploadAndFinish(document.toInputStream(), progressListener);
            }
        } catch (RateLimitException exception) {
            try {
                Thread.sleep(2000);
                writeDocument(document, path);
            } catch (InterruptedException sleepEx) {
                Thread.currentThread().interrupt();
            }
        } catch (DbxException e) {
            throw new IOException(String.format(
                "There has been a conflict because a file with the name %s already exists at %s.\n" +
                    "To overwrite existing files use write mode Overwrite instead of Add.",
                document.getName(),
                path));
        }
    }

    /**
     * A special write operation specifically for files larger than 8 MB. The file is uploaded in chunks to
     * reduce the number of requests to the Dropbox server.
     * Example taken from:
     * <a href="https://github.com/dropbox/dropbox-sdk-java/blob/main/examples/examples/src/main/java/com/dropbox/core/examples/upload_file/UploadFileExample.java">Dropbox Chunked upload example</a>
     *
     * @param document         The document to be written.
     * @param path             The path where the document should be written to
     * @param progressListener A progress listener to track the write progress of the document.
     * @throws IOException If an error occurs, throws the error as an IOException.
     */
    private void writeDocumentChunked(DUUIDocument document, String path, IOUtil.ProgressListener progressListener) throws IOException {
        long size = document.getSize();
        long uploadProgress = 0L;

        String sessionId = null;

        for (int i = 0; i < MAX_RETRIES; ++i) {
            if (i > 0 && debug) {
                System.out.printf("Upload try %d of %d.", i + 1, MAX_RETRIES);
            }

            try (InputStream stream = document.toInputStream()) {
                // If this is not the first try. Skip already uploaded bytes.
                long ignored = stream.skip(uploadProgress);

                // Initialize the sessionId and start the upload.
                if (sessionId == null) {
                    sessionId = client
                        .files()
                        .uploadSessionStart()
                        .uploadAndFinish(stream, CHUNK_SIZE, progressListener)
                        .getSessionId();
                    uploadProgress += CHUNK_SIZE;
                }

                UploadSessionCursor cursor = new UploadSessionCursor(sessionId, uploadProgress);

                while ((size - uploadProgress) > CHUNK_SIZE) {
                    client
                        .files()
                        .uploadSessionAppendV2(cursor)
                        .uploadAndFinish(stream, CHUNK_SIZE, progressListener);

                    uploadProgress += CHUNK_SIZE;
                    cursor = new UploadSessionCursor(sessionId, uploadProgress);
                }

                long remaining = size - uploadProgress;
                CommitInfo commitInfo = CommitInfo
                    .newBuilder(path)
                    .withMode(writeMode)
                    .build();

                client
                    .files()
                    .uploadSessionFinish(cursor, commitInfo)
                    .uploadAndFinish(stream, remaining, progressListener);

            } catch (DbxException exception) {
                throw new IOException(exception);
            }
        }

    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) throws IOException {
        for (DUUIDocument document : documents) {
            writeDocument(document, path);
        }
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        ByteArrayOutputStream fileContentOutput = new ByteArrayOutputStream();
        DUUIDocument document = new DUUIDocument(Paths.get(path).getFileName().toString(), path);
        IOUtil.ProgressListener progressListener = new IOUtil.ProgressListener() {
            long downloadProgress = 0;

            @Override
            public void onProgress(long progress) {
                document.setDownloadProgress(progress + downloadProgress);
                if (progress == CHUNK_SIZE) downloadProgress += CHUNK_SIZE;
                if (debug) System.out.printf("Bytes downloaded: %d", document.getDownloadProgress());
            }
        };

        try {
            DbxDownloader<FileMetadata> downloader = client
                .files()
                .download(addLeadingSlashToPath(path));

            downloader.download(fileContentOutput, progressListener);
            fileContentOutput.close();

            FileMetadata metadata = downloader.getResult();

            document.setName(metadata.getName());
            document.setPath(metadata.getPathLower());
            document.setBytes(fileContentOutput.toByteArray());
            document.setSize(metadata.getSize());

            return document;
        } catch (DbxException e) {
            throw new IOException(e);
        }

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
        ListFolderResult result;

        try {
            result = client
                .files()
                .listFolderBuilder(path)
                .withRecursive(recursive)
                .start();

        } catch (DbxException e) {
            throw new IOException(e);
        }

        return result
            .getEntries()
            .stream()
            .filter(metadata
                -> metadata.getPathLower().endsWith(fileExtension)
                && metadata instanceof FileMetadata) // Only FileMetadata includes the size since folders have no size.
            .map(metadata
                -> new DUUIDocument(
                metadata.getName(),
                metadata.getPathLower(),
                ((FileMetadata) metadata).getSize()))
            .collect(Collectors.toList());

    }

    @Override
    public DUUIFolder getFolderStructure() {
        return getFolderStructure("", "Files");
    }

    public DUUIFolder getFolderStructure(String path, String name) {

        DUUIFolder root = new DUUIFolder(path, name);

        ListFolderResult result = null;

        try {
            result = client
                    .files()
                    .listFolderBuilder(path)
                    .start();

        } catch (DbxException e) {
            return null;
        }

        result.getEntries().stream()
            .filter(f -> f instanceof FolderMetadata)
            .map(f -> getFolderStructure(((FolderMetadata) f).getPathLower(), f.getName()))
            .filter(Objects::nonNull)
            .forEach(root::addChild);

        return root;
    }
}