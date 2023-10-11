package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelinePerformancePoint;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;

import java.net.UnknownHostException;
import java.time.Instant;
import java.util.HashMap;

public class DUUIMongoDBStorageBackend implements IDUUIStorageBackend {

    private final boolean trackErrorDocs;
    private final MongoClient _client;

    public DUUIMongoDBStorageBackend(String connectionURI, boolean trackErrorDocs) {
        this.trackErrorDocs = trackErrorDocs;
        _client = MongoClients.create(connectionURI);
    }

    public DUUIMongoDBStorageBackend(String connectionURI) {
        this(connectionURI, false);
    }

    @Override
    public void addNewRun(String name, DUUIComposer composer) {
        MongoDatabase database = this._client.getDatabase("duui_metrics");
        MongoCollection<Document> pipelineCollection = database.getCollection("pipeline");
        MongoCollection<Document> performanceCollection = database.getCollection("pipeline_perf");
        MongoCollection<Document> documentPerformanceCollection = database.getCollection("pipeline_document_perf");
        MongoCollection<Document> componentCollection = database.getCollection("pipeline_component");

        pipelineCollection.findOneAndDelete(Filters.eq("name", name));
        performanceCollection.findOneAndDelete(Filters.eq("name", name));
        documentPerformanceCollection.findOneAndDelete(Filters.eq("pipelinename", name));
        componentCollection.findOneAndDelete(Filters.eq("name", name));

        pipelineCollection.insertOne(
                new Document("name", name)
                        .append("workers", composer.getWorkerCount())
        );

        for (DUUIPipelineComponent component : composer.getPipeline()) {
            String description = component.toJson();
            long hash = component.getFinalizedRepresentationHash();
            componentCollection.insertOne(
                    new Document("hash", hash)
                            .append("name", name)
                            .append("description", description));
        }

    }

    @Override
    public void addMetricsForDocument(DUUIPipelineDocumentPerformance perf) {
        MongoDatabase database = this._client.getDatabase("duui_metrics");
        MongoCollection<Document> documentCollection = database.getCollection("pipeline_document");
        MongoCollection<Document> documentPerformanceCollection = database.getCollection("pipeline_document_perf");

        documentCollection.insertOne(
                new Document("documentSize",            perf.getDocumentSize())
                        .append("waitTime",             perf.getDocumentWaitTime())
                        .append("totalTime",            perf.getTotalTime())
                        .append("document",             perf.getDocument())
                        .append("annotationsTypeCount", perf.getAnnotationTypesCount())
        );

        for (DUUIPipelinePerformancePoint point : perf.getPerformancePoints()) {
            documentPerformanceCollection.insertOne(
                    new Document("pipelinename", perf.getRunKey())
                            .append("componenthash",          point.getKey())
                            .append("durationSerialize",      point.getDurationSerialize())
                            .append("durationDeserialize",    point.getDurationDeserialize())
                            .append("durationAnnotator",      point.getDurationAnnotator())
                            .append("durationMutexWait",      point.getDurationMutexWait())
                            .append("durationComponentTotal", point.getDurationComponentTotal())
                            .append("totalAnnotations",       point.getNumberOfAnnotations())
                            .append("documentSize",           point.getDocumentSize())
                            .append("serializedSize",         point.getSerializedSize())
                            .append("error",                  point.getError())
                            .append("document",               point.getDocument())
            );
        }
    }

    /**
     * Populates a IDUUIPipelineComponent from a HashMap of options that is loaded from the MongoDB
     * storage backend. If not options are present an emtpy component is returned instead.
     *
     * @param hash The finalized component hash.
     * @return Populated IDUUIPipelineComponent from options stored in MongoDB.
     */
    @Override
    public IDUUIPipelineComponent loadComponent(String hash) {
        MongoDatabase database = this._client.getDatabase("duui_metrics");
        MongoCollection<Document> collection = database.getCollection("pipeline_component");
        Document component = collection.find(Filters.eq("hash", Long.parseLong(hash))).first();
        if (component == null) {
            return new IDUUIPipelineComponent();
        }

        Document options = Document.parse(component.getString("description")).get("options", Document.class);
        if (options == null) {
            return new IDUUIPipelineComponent();
        }

        HashMap<String, Object> optionsMap = new HashMap<>(options);
        return new IDUUIPipelineComponent(optionsMap);
    }


    @Override
    public void finalizeRun(String name, Instant start, Instant end) {
        MongoDatabase database = this._client.getDatabase("duui_metrics");
        MongoCollection<Document> performanceCollection = database.getCollection("pipeline_perf");

        performanceCollection.insertOne(
                new Document("name", name)
                        .append("startTime", start.toEpochMilli())
                        .append("endTime", end.toEpochMilli())
        );
    }

    @Override
    public void shutdown() throws UnknownHostException {
        System.out.print("[DUUIMongoStorageBackend] Shutting down.\n");
        // TODO Should something happen here?
    }

    @Override
    public boolean shouldTrackErrorDocs() {
        return trackErrorDocs;
    }
}
