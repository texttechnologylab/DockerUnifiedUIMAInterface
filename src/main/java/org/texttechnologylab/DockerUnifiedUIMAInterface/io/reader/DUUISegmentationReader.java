package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionDBReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.ProgressMeter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.format.IDUUIFormat;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.format.TxtLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport.IDUUITransport;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport.LocalFile;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyNone;
import org.texttechnologylab.annotation.AnnotationComment;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DUUISegmentationReader implements DUUICollectionDBReader {
    private static final String MONGO_BUCKET_NAME = "duui_segmented_documents";
    private static final String MONGO_BUCKET_NAME_FILES = MONGO_BUCKET_NAME + ".files";

    private static final String DUUI_SEGMENTATION_READER_SEGMENT_ID = "__duui_segmentation_reader_segment_id__";

    private final MongoCollection mongoCollection;
    private final GridFSBucket mongoBucket;
    private final DUUISegmentationStrategy segmentationStrategy;
    private final ProgressMeter progress;
    private final AtomicBoolean loadingFinished;

    protected static String getGridId(String docId, long segmentIndex) {
        return docId + "_" + segmentIndex;
    }

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

            // TODO use single connection (pool?) for all threads?
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
                    System.out.println(Thread.currentThread().getId() + " - done " + docCounter + ": " + path);
                }
            }
        }

        private void store(JCas jCas, String docId, long segmentIndex) {
            DocumentMetaData meta = DocumentMetaData.get(jCas);

            // TODO multiple compression methods
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .metadata(new Document()
                            .append("type", "uima")
                            .append("compressed", true)
                            .append("compression", "gzip")
                            .append("duui_document_id", docId)
                            .append("duui_segment_index", segmentIndex)
                            .append("document_id", meta.getDocumentId())
                            .append("document_uri", meta.getDocumentUri())
                            .append("document_base_uri", meta.getDocumentBaseUri())
                            .append("document_title", meta.getDocumentTitle())
                            .append("collection_id", meta.getCollectionId())
                            .append("duui_status_finished", false)
                            .append("duui_status_tools", new ArrayList<>())
                    );

            String segmentId = getGridId(docId, segmentIndex);
            try(GridFSUploadStream upload = this.mongoBucket.openUploadStream(segmentId, options)) {

                // Add metainfo
                // TODO use special annotation type?
                AnnotationComment comment = new AnnotationComment(jCas);
                comment.setKey(DUUI_SEGMENTATION_READER_SEGMENT_ID);
                comment.setValue(segmentId);
                comment.addToIndexes();

                // TODO different compressions using io/transport/format
                try (GZIPOutputStream gzip = new GZIPOutputStream(upload)) {
                    XmiCasSerializer.serialize(jCas.getCas(), gzip);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public DUUISegmentationReader(Path sourcePath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy, int workers) throws IOException, InterruptedException {
        this(sourcePath, mongoConfig, segmentationStrategy, workers, Integer.MAX_VALUE);
    }

    public DUUISegmentationReader(Path sourcePath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy, int workers, int capacity) {
        this.segmentationStrategy = segmentationStrategy;

        MongoDBConnectionHandler mongoConnectionHandler = new MongoDBConnectionHandler(mongoConfig);
        this.mongoCollection = mongoConnectionHandler.getCollection(MONGO_BUCKET_NAME_FILES);
        this.mongoBucket = GridFSBuckets.create(mongoConnectionHandler.getDatabase(), MONGO_BUCKET_NAME);

        this.progress = new ProgressMeter(getSize());

        this.loadingFinished = new AtomicBoolean(false);

        new Thread(() -> {
            try {
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
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourcePath)) {
                    for (Path entry : stream) {
                        //System.out.println("Adding " + entry);
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
            }
            catch (Exception e) {
                e.printStackTrace();
                System.err.println("CollectionReader failed!");
            }
            finally {
                loadingFinished.set(true);
            }
        }).start();
    }

    @Override
    public boolean finishedLoading() {
        return loadingFinished.get();
    }

    @Override
    public ProgressMeter getProgress() {
        return progress;
    }

    @Override
    public void getNextCas(JCas pCas) {
        // TODO just give unfinished ones?
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean getNextCas(JCas pCas, String toolUUID, int pipelinePosition) {
        pCas.reset();

        // TODO document locking

        // Not finished, has n tools processed, and not own tool
        Bson notFinishedFilter = Filters.ne("finished", true);
        Bson sizeCondition = Filters.expr(new Document("$gte", Arrays.asList(new Document("$size", "$metadata.duui_status_tools"), pipelinePosition)));
        Bson toolExistsCondition = Filters.ne("metadata.duui_status_tools", toolUUID);
        Bson query = Filters.and(notFinishedFilter, sizeCondition, toolExistsCondition);
        Document next = (Document) mongoCollection.find(query).first();
        // TODO fehlerhandling!
        if (next == null) {
            return false;
        }

        String segmentId = next.getString("filename");
        GridFSDownloadOptions options = new GridFSDownloadOptions();
        try(GridFSDownloadStream download = mongoBucket.openDownloadStream(segmentId, options)) {
            try (GZIPInputStream gzip = new GZIPInputStream(download)) {
                XmiCasDeserializer.deserialize(gzip, pCas.getCas());
            }
        } catch (IOException | SAXException e) {
            throw new RuntimeException(e);
        }

        progress.setLimit(getSize());
        progress.setDone(getDone());
        return true;
    }

    public void updateCas(JCas pCas, String toolUUID, boolean status, List<String> pipelineUUIDs) {
        String segmentId = JCasUtil.select(pCas, AnnotationComment.class).stream()
                .filter(c -> c.getKey().equals(DUUI_SEGMENTATION_READER_SEGMENT_ID))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No segment id found!"))
                .getValue();

        // Delete all (should only be one)
        // TODO merge multiple
        Document docMeta = null;
        for (GridFSFile doc : mongoBucket.find(new Document("filename", segmentId))) {
            if (docMeta == null) {
                docMeta = doc.getMetadata();
            }
            mongoBucket.delete(doc.getObjectId());
        }

        List<String> toolUUIDs = docMeta.get("duui_status_tools", List.class);
        toolUUIDs.add(toolUUID);

        boolean finished = new HashSet<>(toolUUIDs).containsAll(pipelineUUIDs);
        docMeta.append("duui_status_finished", finished);

        GridFSUploadOptions options = new GridFSUploadOptions()
                .metadata(docMeta);

        try(GridFSUploadStream upload = this.mongoBucket.openUploadStream(segmentId, options)) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(upload)) {
                XmiCasSerializer.serialize(pCas.getCas(), gzip);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return mongoCollection.countDocuments(new Document("metadata.duui_status_finished", false)) > 0;
    }

    @Override
    public long getSize() {
        return mongoCollection.countDocuments();
    }

    @Override
    public long getDone() {
        return mongoCollection.countDocuments(new Document("metadata.duui_status_finished", true));
    }
}
