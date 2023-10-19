package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasIOUtils;
import org.bson.Document;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;
import org.texttechnologylab.annotation.AnnotationComment;

import java.io.IOException;

public class GerParCorWriter extends JCasFileWriter_ImplBase {

    public static final String PARAM_DBConnection = "dbconnection";
    private final String GRIDID = "gridid";
    @ConfigurationParameter(name = PARAM_DBConnection, mandatory = true)
    protected String dbconnection;

    MongoDBConnectionHandler dbConnectionHandler = null;
    GridFSBucket gridFS = null;

    @Override
    public void initialize(UimaContext aContext) throws ResourceInitializationException {
        super.initialize(aContext);

        MongoDBConfig dbConfig = null;
        try {
            dbConfig = new MongoDBConfig(dbconnection);
            dbConnectionHandler = new MongoDBConnectionHandler(dbConfig);
            this.gridFS = GridFSBuckets.create(dbConnectionHandler.getDatabase(), "grid");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {

        String sGridId = "";

        try {
            AnnotationComment pGridID = JCasUtil.select(aJCas, AnnotationComment.class).stream().filter(ac -> {
                return ac.getKey().equals(GRIDID);
            }).findFirst().get();
            if (pGridID != null) {
                sGridId = pGridID.getValue();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        if (sGridId.length() > 0) {

            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(358400)
                    .metadata(new Document("type", "uima"))
                    .metadata(new Document(GRIDID, sGridId));

            GridFSUploadStream uploadStream = gridFS.openUploadStream(sGridId, options);
            try {
                CasIOUtils.save(aJCas.getCas(), uploadStream, SerialFormat.XMI_1_1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

    }
}
