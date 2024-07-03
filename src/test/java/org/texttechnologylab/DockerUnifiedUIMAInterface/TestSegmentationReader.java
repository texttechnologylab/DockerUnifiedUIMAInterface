package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUISegmentationReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestSegmentationReader {

    private enum Tasks {
        FLAIR,
        SENTIMENT
    }

    @Test
    public void testAll() throws Exception {
        List<Integer> delimiterRange = List.of(5000, 200, 500, 1000, 10_000, 20_000, 50_000);
        for (int delimiter : delimiterRange) {
            testBase("gerparcor_sample1000_RANDOM_100", delimiter, Tasks.SENTIMENT);
            System.gc();
        }
    }

    public void testBase(String corpus, int delimeterSize, Tasks task) throws Exception {
        int toolWorkers = 1;

        int segmentationWorkers = 1;
        int segmentationQueueSize = Integer.MAX_VALUE;

        Path out = Paths.get("/opt/sample/out/" + corpus + "_" + delimeterSize + "_" + task.name());
        Files.createDirectory(out);
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("./benchmark.db")
                .withConnectionPoolSize(toolWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withWorkers(toolWorkers)
                .withStorageBackend(sqlite);

        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(swarmDriver);
        composer.addDriver(remoteDriver);

        MongoDBConfig mongoConfig = new MongoDBConfig("segmentation_mongo.properties");

        DUUISegmentationStrategy segmentationStrategy = new DUUISegmentationStrategyByDelemiter()
                .withLength(delimeterSize)
                .withDelemiter(".");

        DUUISegmentationReader reader = new DUUISegmentationReader(
                Paths.get("/opt/sample/" + corpus),
                out,
                mongoConfig,
                segmentationStrategy,
                segmentationWorkers,
                segmentationQueueSize
        );

        System.out.println("Size: " + reader.getSize());
        System.out.println("Done: " + reader.getDone());
        System.out.println("Progress: " + reader.getProgress());


        switch (task) {
            case FLAIR:
                composer.add(
                        new DUUIRemoteDriver
                                .Component("http://isengart.hucompute.org:8100")
                                .withScale(toolWorkers)
                );
                break;
            case SENTIMENT:
                composer.add(
                        new DUUISwarmDriver
                                .Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:latest")
                                .withScale(toolWorkers)
                                .withParameter("model_name", "cardiffnlp/twitter-roberta-base-sentiment")
                                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
                                .withConstraintHost("isengart")
                );
                break;
        }
        composer.runSegmented(reader, corpus + "_" + delimeterSize + "_" + task.name());
        composer.shutdown();
    }

    @Test
    void testSegmentationReader() throws Exception {
        int toolWorkers = 1;

        int segmentationWorkers = 1;
        int segmentationQueueSize = Integer.MAX_VALUE;

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("./test.db")
                .withConnectionPoolSize(toolWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withWorkers(toolWorkers)
                .withStorageBackend(sqlite);

        DUUISwarmDriver remoteDriver = new DUUISwarmDriver();
        composer.addDriver(remoteDriver);

        MongoDBConfig mongoConfig = new MongoDBConfig("segmentation_mongo.properties");

        DUUISegmentationStrategy segmentationStrategy = new DUUISegmentationStrategyByDelemiter()
                .withLength(500)
                .withDelemiter(".");

        DUUISegmentationReader reader = new DUUISegmentationReader(
                Paths.get("/opt/sample/gerparcor_sample1000_SMALLEST_100/Bayern"),
                Paths.get("/opt/sample/out"),
                mongoConfig,
                segmentationStrategy,
                segmentationWorkers,
                segmentationQueueSize
        );

        System.out.println("Size: " + reader.getSize());
        System.out.println("Done: " + reader.getDone());
        System.out.println("Progress: " + reader.getProgress());

        composer.add(
                new DUUISwarmDriver
                        .Component("docker.texttechnologylab.org/duui-spacy-de_core_news_sm:0.4.1")
                        .withScale(toolWorkers)
                        .withConstraintHost("isengart")
        );

        composer.runSegmented(reader, "test1");
        composer.shutdown();
    }
}
