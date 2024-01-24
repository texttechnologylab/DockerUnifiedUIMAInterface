package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import io.minio.*;
import io.minio.credentials.Provider;
import io.minio.messages.Item;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
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
    public void writeDocument(DUUIDocument document, String bucket) throws IOException {
        boolean doesBucketExist;

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

            client
                .putObject(
                    PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(document.getOutputName())
                        .stream(
                            document.getResult(),
                            document.getOutputStream().size(),
                            -1)
                        .build());

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
        Path p = Paths.get(path);
        String bucket = p.getParent().toString();
        String name = p.getFileName().toString();

        try (InputStream document = client.getObject(GetObjectArgs
            .builder()
            .bucket(bucket)
            .object(name)
            .build())) {
            return new DUUIDocument(name, bucket, document.readAllBytes());
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
    public List<DUUIDocument> listDocuments(String bucket, String fileExtension, boolean recursive) throws IOException {
        Iterable<Result<Item>> results = client.listObjects(
            ListObjectsArgs
                .builder()
                .bucket(bucket)
                .recursive(recursive)
                .build()
        );

        List<DUUIDocument> documents = new ArrayList<>();
        for (Result<Item> result : results) {
            try {
                documents.add(
                    new DUUIDocument(
                        result.get().objectName(),
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
