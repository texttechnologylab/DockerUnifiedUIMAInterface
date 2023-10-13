package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.commons.io.IOUtils;


public class DUUIS3DataReader implements IDUUIDataReader {

    private final AmazonS3 s3client;

    public DUUIS3DataReader() {
        s3client = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
    }

    public DUUIS3DataReader(Regions region) {
        s3client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    }

    @Override
    public void writeFile(DUUIInputStream stream, String bucketName) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
        try {
            xfer_mgr.upload(bucketName, stream.getName(), new File(stream.getPath()));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    @Override
    public void writeFiles(List<DUUIInputStream> streams, String bucketName) {
        for (DUUIInputStream file : streams) {
            writeFile(file, bucketName);
        }
    }

    @Override
    public DUUIInputStream readFile(String source) {
        return null;
    }

    @Override
    public List<DUUIInputStream> readFiles(List<String> paths) throws IOException {
        return null;
    }

    public DUUIInputStream readFile(String bucketName, String keyName) {
        try {
            S3Object object = s3client.getObject(bucketName, keyName);
            return new DUUIInputStream(
                keyName,
                bucketName,
                object.getObjectMetadata().getContentLength(),
                new ByteArrayInputStream(IOUtils.toByteArray(object.getObjectContent()))
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listFiles(String bucketName) {
        ListObjectsV2Result result = s3client.listObjectsV2(bucketName);
        return result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    @Override
    public List<String> listFiles(String bucketName, String fileExtension) throws IOException {
        ListObjectsV2Result result = s3client.listObjectsV2(bucketName);
        return result.getObjectSummaries()
            .stream()
            .map(S3ObjectSummary::getKey)
            .filter(key -> key.endsWith(fileExtension))
            .collect(Collectors.toList());
    }

    @Override
    public List<String> listFiles(String bucketName, String fileExtension, boolean recursive) throws IOException {
        return listFiles(bucketName, fileExtension);
    }

}
