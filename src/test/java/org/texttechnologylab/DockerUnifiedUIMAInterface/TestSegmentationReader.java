package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.util.InvalidXMLException;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUISegmentationReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class TestSegmentationReader {
    @Test
    void testSegmentationReader() throws Exception {
        int toolWorkers = 1;

        int segmentationWorkers = 2;
        int segmentationQueueSize = Integer.MAX_VALUE;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withWorkers(toolWorkers);

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        MongoDBConfig mongoConfig = new MongoDBConfig("segmentation_mongo.properties");

        DUUISegmentationStrategy segmentationStrategy = new DUUISegmentationStrategyByDelemiter()
                .withLength(50)
                .withDelemiter(".");

        DUUISegmentationReader reader = new DUUISegmentationReader(
                Paths.get("src/test/resources/org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation/segmentation/txt"),
                mongoConfig,
                segmentationStrategy,
                segmentationWorkers,
                segmentationQueueSize
        );

        System.out.println("Size: " + reader.getSize());
        System.out.println("Done: " + reader.getDone());
        System.out.println("Progress: " + reader.getProgress());

        composer.add(
                new DUUIRemoteDriver
                        .Component("http://127.0.0.1:9714")
                        .withScale(toolWorkers)
        );

        composer.runSegmented(reader, "test1");
    }
}
