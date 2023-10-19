package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.Progress;
import org.bson.BsonDocument;
import org.bson.Document;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;

import java.io.IOException;

public class GerParCorReader extends CasCollectionReader_ImplBase {

    public static final String PARAM_DBConnection = "dbconnection";
    public static final String PARAM_Query = "query";
    private final String GRIDID = "gridid";
    @ConfigurationParameter(name = PARAM_DBConnection, mandatory = true)
    protected String dbconnection;
    @ConfigurationParameter(name = PARAM_Query, mandatory = false, defaultValue = "{}")
    protected String query;


    MongoDBConnectionHandler dbConnectionHandler = null;
    GridFSBucket gridFS = null;

    private MongoCursor<Document> results = null;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        MongoDBConfig dbConfig = null;
        try {
            dbConfig = new MongoDBConfig(dbconnection);
            dbConnectionHandler = new MongoDBConnectionHandler(dbConfig);
            this.gridFS = GridFSBuckets.create(dbConnectionHandler.getDatabase(), "grid");
            results = dbConnectionHandler.getCollection().find(BsonDocument.parse(query)).cursor();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getNext(CAS aCAS) throws IOException, CollectionException {
        Document pDocument = results.next();

        String gridID = pDocument.getString("grid");

        try (GridFSDownloadStream downloadStream = gridFS.openDownloadStream(gridID)) {
            CasIOUtils.load(downloadStream, aCAS);
        }
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        return results.hasNext();
    }

    @Override
    public Progress[] getProgress() {
        return new Progress[0];
    }


}
