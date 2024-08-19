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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionDBReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.format.IDUUIFormat;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.format.XmiLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport.GZIPLocalFile;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.transport.IDUUITransport;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyNone;
import org.texttechnologylab.annotation.AnnotationComment;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private final MongoDBConfig mongoDBConfig;
    private final MongoCollection<Document> mongoCollection;
    private final GridFSBucket mongoBucket;
    private final DUUISegmentationStrategy segmentationStrategy;
    private final AdvancedProgressMeter progress;
    private final AtomicBoolean loadingFinished;
    private final Path outPath;
    private final int workers;
    private final int capacity;

    protected static String getGridId(String docId, long segmentIndex) {
        return docId + "_" + segmentIndex;
    }

    static class PathMessage {
        Path path;

        public PathMessage(Path path) {
            this.path = path;
        }
    }

    static class MergeMessage {
        public String id;
        public List<ObjectId> ids;

        public MergeMessage(String id, List<ObjectId> ids) {
            this.id = id;
            this.ids = ids;
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
                        IDUUITransport transport = new GZIPLocalFile(path);
                        IDUUIFormat format = new XmiLoader(true);
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
                    } catch (Exception e) {
                        e.printStackTrace();
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
            try (GridFSUploadStream upload = this.mongoBucket.openUploadStream(segmentId, options)) {

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

    static class Merger implements Runnable {
        private final BlockingQueue<MergeMessage> queue;
        private final GridFSBucket mongoBucket;
        private final DUUISegmentationStrategy segmentationStrategy;
        private final Path outPath;

        public Merger(BlockingQueue<MergeMessage> queue, Path outPath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy) {
            this.queue = queue;
            this.outPath = outPath;
            // TODO use single connection (pool?) for all threads?
            MongoDBConnectionHandler mongoConnectionHandler = new MongoDBConnectionHandler(mongoConfig);

            this.mongoBucket = GridFSBuckets.create(mongoConnectionHandler.getDatabase(), MONGO_BUCKET_NAME);

            this.segmentationStrategy = segmentationStrategy;
        }

        @Override
        public void run() {
            JCas jCas;
            try {
                jCas = JCasFactory.createJCas();
            } catch (UIMAException e) {
                throw new RuntimeException(e);
            }

            while (true) {
                MergeMessage message;
                try {
                    message = queue.poll(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (message != null) {
                    if (message.id == null)
                        break;
                    try {
                        boolean first = true;

                        for (ObjectId id : message.ids) {
                            // Bit ugly but it's fine for now...
                            if (first) {
                                fetch(id, jCas);
                                segmentationStrategy.initialize(jCas);
                                first = false;
                            } else {
                                JCas currentCas = JCasFactory.createJCas();
                                fetch(id, currentCas);
                                segmentationStrategy.merge(currentCas);
                            }
                        }
                    } catch (UIMAException e) {
                        throw new RuntimeException(e);
                    }
                    String dir = String.format("%s/%s", this.outPath.toString(), getRelativePath(jCas, true, false));
                    Path path = Paths.get(dir + ".xmi.gz");
                    try {
                        Files.createDirectories(Paths.get(dir.substring(0, dir.lastIndexOf("/"))));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    try (GZIPOutputStream stream = new GZIPOutputStream(Files.newOutputStream(path))) {
                        XmiCasSerializer.serialize(jCas.getCas(), stream);
                    } catch (SAXException | IOException e) {
                        throw new RuntimeException(e);
                    }

                    jCas.reset();
                }
            }
        }

        private void fetch(ObjectId id, JCas cas) {
            cas.reset();
            try (GridFSDownloadStream download = this.mongoBucket.openDownloadStream(id)) {

                // TODO different compressions using io/transport/format
                try (GZIPInputStream gzip = new GZIPInputStream(download)) {
                    XmiCasDeserializer.deserialize(gzip, cas.getCas());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public DUUISegmentationReader(Path sourcePath, Path outPath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy, int workers) throws IOException, InterruptedException {
        this(sourcePath, outPath, mongoConfig, segmentationStrategy, workers, Integer.MAX_VALUE);
    }

    public DUUISegmentationReader(Path sourcePath, Path outPath, MongoDBConfig mongoConfig, DUUISegmentationStrategy segmentationStrategy, int workers, int capacity) {
        this.segmentationStrategy = segmentationStrategy;
        this.mongoDBConfig = mongoConfig;
        this.outPath = outPath;
        this.workers = workers;
        this.capacity = capacity;
        MongoDBConnectionHandler mongoConnectionHandler = new MongoDBConnectionHandler(mongoConfig);
        this.mongoCollection = mongoConnectionHandler.getCollection(MONGO_BUCKET_NAME_FILES);
        // TODO: Make this configurable
        GridFSBuckets.create(mongoConnectionHandler.getDatabase(), MONGO_BUCKET_NAME).drop();
        this.mongoBucket = GridFSBuckets.create(mongoConnectionHandler.getDatabase(), MONGO_BUCKET_NAME);

        this.progress = new AdvancedProgressMeter(getSize());

        this.loadingFinished = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                BlockingQueue<PathMessage> queue = new LinkedBlockingDeque<>(capacity);

                System.out.println("Starting " + workers + " workers to segment files...");
                List<Thread> segmenterThreads = new ArrayList<>();
                for (int i = 0; i < workers; i++) {
                    Thread thread = new Thread(new Segmenter(queue, this.mongoDBConfig, SerializationUtils.clone(segmentationStrategy)));
                    thread.start();
                    segmenterThreads.add(thread);
                }

                System.out.println("Collecting files from " + sourcePath + "...");
                long counter = addFilesInFolder(sourcePath, queue);
                System.out.println("Added " + counter + " files to the queue.");

                System.out.println("Waiting for workers to finish...");
                for (Thread ignored : segmenterThreads) {
                    queue.put(new PathMessage(null));
                }
                for (Thread thread : segmenterThreads) {
                    thread.join();
                }
                System.out.println("All workers finished.");
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("CollectionReader failed!");
            } finally {
                loadingFinished.set(true);
            }
        }).start();
    }

    private int addFilesInFolder(Path sourcePath, BlockingQueue<PathMessage> queue) {
        int counter = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourcePath)) {
            for (Path entry : stream) {
                if (entry.toFile().isDirectory()) {
                    counter += addFilesInFolder(entry, queue);
                } else {
                    queue.put(new PathMessage(entry));
                    counter++;
                }
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
        return counter;
    }

    @Override
    public boolean finishedLoading() {
        return loadingFinished.get();
    }

    @Override
    public AdvancedProgressMeter getProgress() {
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

        // Not finished, has n tools processed, and not own tool
        Bson notFinishedFilter = Filters.ne("finished", true);
        Bson sizeCondition = Filters.expr(new Document("$gte", Arrays.asList(new Document("$size", "$metadata.duui_status_tools"), pipelinePosition)));
        Bson toolExistsCondition = Filters.ne("metadata.duui_status_tools", toolUUID);
        Bson notLockedCondition = Filters.ne("metadata.duui_locked", true);
        Bson query = Filters.and(notFinishedFilter, sizeCondition, toolExistsCondition, notLockedCondition);
        Document next = mongoCollection.findOneAndUpdate(query, new Document("$set", new Document("metadata.duui_locked", true)));
        // TODO fehlerhandling!
        if (next == null) {
            return false;
        }

        String segmentId = next.getString("filename");
        GridFSDownloadOptions options = new GridFSDownloadOptions();
        try (GridFSDownloadStream download = mongoBucket.openDownloadStream(segmentId, options)) {
            try (GZIPInputStream gzip = new GZIPInputStream(download)) {
                XmiCasDeserializer.deserialize(gzip, pCas.getCas());
            }
        } catch (IOException | SAXException e) {
            throw new RuntimeException(e);
        }

        progress.setMax(getSize());
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

        docMeta.append("duui_locked", false);

        GridFSUploadOptions options = new GridFSUploadOptions()
                .metadata(docMeta);

        try (GridFSUploadStream upload = this.mongoBucket.openUploadStream(segmentId, options)) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(upload)) {
                XmiCasSerializer.serialize(pCas.getCas(), gzip);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: Create a separate thread that merges files
    public void merge() {
        Bson sort = new Document("metadata.duui_document_id", 1).append("metadata.duui_segment_index", 1);
        Bson group = new Document("_id", "$metadata.duui_document_id").append("ids", new Document("$push", "$_id"));

        try {
            // TODO: Capacity
            BlockingQueue<MergeMessage> queue = new LinkedBlockingDeque<>(this.capacity);

            System.out.println("Starting " + this.workers + " workers to merge files...");
            List<Thread> mergerThreads = new ArrayList<>();
            for (int i = 0; i < this.workers; i++) {
                Thread thread = new Thread(new Merger(queue, this.outPath, this.mongoDBConfig, SerializationUtils.clone(segmentationStrategy)));
                thread.start();
                mergerThreads.add(thread);
            }
            for (Document doc : mongoCollection.aggregate(List.of(new Document("$sort", sort), new Document("$group", group)))) {
                queue.put(new MergeMessage(doc.getString("_id"), doc.getList("ids", ObjectId.class)));
            }
            // Add termination messages
            for (Thread ignored : mergerThreads) {
                queue.put(new MergeMessage(null, null));
            }
            System.out.println("Waiting for workers to finish...");
            for (Thread thread : mergerThreads) {
                thread.join();
            }
            System.out.println("All workers finished.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("CollectionReader failed!");
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

    protected static String getRelativePath(JCas aJCas, boolean stripExtension, boolean escapeFilename) {
        DocumentMetaData meta = DocumentMetaData.get(aJCas);
        String baseUri = meta.getDocumentBaseUri();
        String docUri = meta.getDocumentUri();

        if (baseUri != null && baseUri.length() != 0) {
            // In some cases, the baseUri may not end with a slash - if so, we add one
            if (baseUri.length() > 0 && !baseUri.endsWith("/")) {
                baseUri += '/';
            }

            if ((docUri == null) || !docUri.startsWith(baseUri)) {
                throw new IllegalStateException("Base URI [" + baseUri
                        + "] is not a prefix of document URI [" + docUri + "]");
            }
            String relativeDocumentPath = docUri.substring(baseUri.length());
            if (stripExtension) {
                relativeDocumentPath = FilenameUtils.removeExtension(relativeDocumentPath);
            }

            // relativeDocumentPath must not start with as slash - if there are any, remove them
            while (relativeDocumentPath.startsWith("/")) {
                relativeDocumentPath = relativeDocumentPath.substring(1);
            }

            if (!escapeFilename) {
                try {
                    relativeDocumentPath = URLDecoder.decode(relativeDocumentPath, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // UTF-8 must be supported on all Java platforms per specification. This should
                    // not happen.
                    throw new IllegalStateException(e);
                }
            }

            return relativeDocumentPath;
        } else {
            if (meta.getDocumentId() == null) {
                throw new IllegalStateException(
                        "Neither base URI/document URI nor document ID set");
            }

            String relativeDocumentPath = meta.getDocumentId();

            if (stripExtension) {
                relativeDocumentPath = FilenameUtils.removeExtension(relativeDocumentPath);
            }

            if (escapeFilename) {
                try {
                    relativeDocumentPath = URLEncoder.encode(relativeDocumentPath, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // UTF-8 must be supported on all Java platforms per specification. This should
                    // not happen.
                    throw new IllegalStateException(e);
                }
            }

            return relativeDocumentPath;
        }
    }
}
