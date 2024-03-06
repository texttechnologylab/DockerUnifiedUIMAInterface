package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import io.minio.*;
import io.minio.credentials.Provider;
import io.minio.messages.Item;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DUUIMinioDocumentHandler implements IDUUIDocumentHandler {
    private final MinioClient client;

    public DUUIMinioDocumentHandler(String endpoint, String accessKey, String secretKey) {
        client = MinioClient
            .builder()
            .endpoint(endpoint)
            .credentials(accessKey, secretKey)
            .build();
    }

    private String getBucketFromPath(String path) {
        String[] parts = path.split("/");
        if (parts.length == 1) return path;
        return parts[0];
    }

    public DUUIMinioDocumentHandler(String endpoint, Provider credentialsProvider) {
        client = MinioClient
            .builder()
            .endpoint(endpoint)
            .credentialsProvider(credentialsProvider)
            .build();
    }

    public DUUIMinioDocumentHandler(Provider credentialsProvider) {
        client = MinioClient
            .builder()
            .credentialsProvider(credentialsProvider)
            .build();
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
        boolean doesBucketExist;
        String bucket = getBucketFromPath(path);

        try {
            doesBucketExist = client.bucketExists(
                BucketExistsArgs
                    .builder()
                    .bucket(bucket)
                    .build());

            if (!doesBucketExist) {
                client.makeBucket(
                    MakeBucketArgs
                        .builder()
                        .bucket(bucket)
                        .build());
            }

            String object = path.replace(bucket, "") + "/" + document.getName();
            if (object.startsWith("/")) {
                object = object.substring(1);
            }

            client
                .putObject(
                    PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(object)
                        .stream(
                            document.toInputStream(),
                            document.getSize(),
                            -1)
                        .build()
                );

        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String bucket) throws IOException {
        for (DUUIDocument document : documents) {
            writeDocument(document, bucket);
        }
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        String bucket = getBucketFromPath(path);
        String object = path.replace(bucket, "");
        if (object.startsWith("/")) object = object.substring(1);
        String name = Paths.get(path).getFileName().toString();

        try (InputStream document = client.getObject(GetObjectArgs
            .builder()
            .bucket(bucket)
            .object(object)
            .build())) {
            return new DUUIDocument(name, path, document.readAllBytes());
        } catch (Exception e) {
            throw new IOException(e);
        }
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
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {
        String bucket = getBucketFromPath(path);
        String prefix = path.replace(bucket, "");
        if (prefix.startsWith("/")) prefix = prefix.substring(1);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix += "/";

        Iterable<Result<Item>> results = client.listObjects(
            ListObjectsArgs
                .builder()
                .bucket(bucket)
                .prefix(prefix)
                .recursive(recursive)
                .build()
        );

        List<DUUIDocument> documents = new ArrayList<>();
        for (Result<Item> result : results) {
            try {
                if (result.get().isDir()) continue;

                documents.add(
                    new DUUIDocument(
                        Paths.get(result.get().objectName()).getFileName().toString(),
                        bucket + "/" + result.get().objectName(),
                        result.get().size()));
            } catch (Exception exception) {
                throw new IOException(exception);
            }
        }

        return documents
            .stream()
            .filter(document -> document.getName().endsWith(fileExtension))
            .collect(Collectors.toList());
    }
}
