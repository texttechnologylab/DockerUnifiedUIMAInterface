package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.format.IDUUIFormat;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.format.TxtLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport.IDUUITransport;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport.LocalFile;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyNone;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class DUUISegmentationReader implements DUUICollectionReader {
    private static final String MONGO_BUCKET_NAME = "duui_segmented_documents";
    private static final String MONGO_BUCKET_NAME_FILES = MONGO_BUCKET_NAME + ".files";

    private final MongoCollection mongoCollection;
    private final GridFSBucket mongoBucket;
    private final DUUISegmentationStrategy segmentationStrategy;
    private final ProgressMeter progress;

    static class PathMessage {
        Path path;

        public PathMessage(Path path) {
            this.path = path;
        }
    }

    static class Segmenter implements Runnable {
        private final BlockingQueue<PathMessage> queue;
        private final GridFSBucket mongoBucket;
        private final DUUISegmentationStrategy segmentationStrategy;

        public Segmenter(BlockingQueue<PathMessage> queue, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy) {
            this.queue = queue;

            // TODO single connection for all threads?
            MongoDBConnectionHandler mongoConnectionHandler = new MongoDBConnectionHandler(mongoConfig);
            this.mongoBucket = GridFSBuckets.create(mongoConnectionHandler.getDatabase(), MONGO_BUCKET_NAME);

            this.segmentationStrategy = segmentationStrategy;

        }

        @Override
        public void run() {
            long docCounter = 0;
            JCas jCas;
            try {
                jCas = JCasFactory.createJCas();
            } catch (UIMAException e) {
                throw new RuntimeException(e);
            }

            while (true) {
                Path path = null;
                try {
                    PathMessage message = queue.poll(10, TimeUnit.SECONDS);
                    if (message != null) {
                        if (message.path == null) {
                            break;
                        }
                        path = message.path;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (path != null) {
                    System.out.println(Thread.currentThread().getId() + " - start " + path);

                    try {
                        IDUUITransport transport = new LocalFile(path);
                        IDUUIFormat format = new TxtLoader("de");
                        format.load(transport.load(), jCas);

                        String docId = UUID.randomUUID().toString();

                        if (segmentationStrategy instanceof DUUISegmentationStrategyNone) {
                            System.err.println("No segmentation strategy set, using full document!");
                        } else {
                            segmentationStrategy.initialize(jCas);
                            long segmentIndex = 0;
                            JCas jCasSegmented = segmentationStrategy.getNextSegment();
                            while (jCasSegmented != null) {
                                store(jCasSegmented, docId, segmentIndex);
                                jCasSegmented = segmentationStrategy.getNextSegment();
                                segmentIndex++;
                            }
                        }
                    } catch (UIMAException | IOException e) {
                        throw new RuntimeException(e);
                    }

                    jCas.reset();
                    docCounter++;
                    System.out.println(Thread.currentThread().getId() + " - done " + path);
                }
            }
        }

        private void store(JCas jCas, String docId, long segmentIndex) {
            DocumentMetaData meta = DocumentMetaData.get(jCas);

            GridFSUploadOptions options = new GridFSUploadOptions()
                    .metadata(new Document()
                            .append("duui_document_id", docId)
                            .append("duui_segment_index", segmentIndex)
                            .append("document_id", meta.getDocumentId())
                            .append("document_uri", meta.getDocumentUri())
                            .append("document_base_uri", meta.getDocumentBaseUri())
                            .append("document_title", meta.getDocumentTitle())
                            .append("collection_id", meta.getCollectionId())
                            .append("duui_status", new Document()
                                    .append("finished", false)
                            )
                    );

            String segmentId = docId + "_" + segmentIndex;

            try(GridFSUploadStream upload = this.mongoBucket.openUploadStream(segmentId, options)) {
                XmiCasSerializer.serialize(jCas.getCas(), upload);
                upload.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public DUUISegmentationReader(Path sourcePath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy, int workers) throws IOException, InterruptedException {
        this(sourcePath, mongoConfig, segmentationStrategy, workers, Integer.MAX_VALUE);
    }

    public DUUISegmentationReader(Path sourcePath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy, int workers, int capacity) throws IOException, InterruptedException {
        this.segmentationStrategy = segmentationStrategy;

        BlockingQueue<PathMessage> queue = new LinkedBlockingDeque<>(capacity);

        System.out.println("Starting " + workers + " workers to segment files...");
        List<Thread> segmenterThreads = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            Thread thread = new Thread(new Segmenter(queue, mongoConfig, SerializationUtils.clone(segmentationStrategy)));
            thread.start();
            segmenterThreads.add(thread);
        }

        System.out.println("Collecting files from " + sourcePath + "...");
        long counter = 0;
        try(DirectoryStream<Path> stream = Files.newDirectoryStream(sourcePath)) {
            for (Path entry : stream) {
                System.out.println("Adding " + entry);
                queue.put(new PathMessage(entry));
                counter++;
            }
        }
        System.out.println("Added " + counter + " files to the queue.");

        System.out.println("Waiting for workers to finish...");
        for (Thread ignored : segmenterThreads) {
            queue.put(new PathMessage(null));
        }
        for (Thread thread : segmenterThreads) {
            thread.join();
        }
        System.out.println("All workers finished.");

        MongoDBConnectionHandler mongoConnectionHandler = new MongoDBConnectionHandler(mongoConfig);
        this.mongoCollection = mongoConnectionHandler.getCollection(MONGO_BUCKET_NAME_FILES);
        this.mongoBucket = GridFSBuckets.create(mongoConnectionHandler.getDatabase(), MONGO_BUCKET_NAME);

        this.progress = new ProgressMeter(getSize());
    }

    @Override
    public ProgressMeter getProgress() {
        return progress;
    }

    @Override
    public void getNextCas(JCas pCas) {

        // TODO hängt eines hinterher?
        progress.setDone(getDone());
    }

    @Override
    public boolean hasNext() {
        return mongoCollection.countDocuments(new Document("metadata.duui_status.finished", false)) > 0;
    }

    @Override
    public long getSize() {
        return mongoCollection.countDocuments();
    }

    @Override
    public long getDone() {
        return mongoCollection.countDocuments(new Document("metadata.duui_status.finished", true));
    }
}
