package org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer;

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
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasIOUtils;
import org.bson.Document;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConnectionHandler;
import org.texttechnologylab.annotation.AnnotationComment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

    private static Set<String> classNames = new HashSet<>(0);

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {

        String sGridId = "";
        String sDocumentId = "";

        try {
            AnnotationComment pGridID = JCasUtil.select(aJCas, AnnotationComment.class).stream().filter(ac -> {
                return ac.getKey().equals(GRIDID);
            }).findFirst().get();
            AnnotationComment pDocumentID = JCasUtil.select(aJCas, AnnotationComment.class).stream().filter(ac -> {
                return ac.getKey().equals("mongoid");
            }).findFirst().get();

            if (pGridID != null) {
                sGridId = pGridID.getValue();
            }
            if (pDocumentID != null) {
                sDocumentId = pDocumentID.getValue();
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

                Document pDocument = this.dbConnectionHandler.getObject(sDocumentId);
                pDocument.put("annotations", countAnnotations(aJCas));

                Document pMeta = pDocument.get("meta", Document.class);
                if (pMeta == null) {
                    pMeta = new Document();
                }
                Document nMeta = getMetaInformation(aJCas);
                for (String s : nMeta.keySet()) {
                    pMeta.put(s, nMeta.get(s));
                }
                pDocument.put("meta", pMeta);

                this.dbConnectionHandler.updateObject(sDocumentId, pDocument);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

    }

    private Document countAnnotations(JCas pCas) {

        Document rDocument = new Document();

        if (classNames.size() > 0) {
            for (String className : classNames) {
                try {
                    Class pClass = Class.forName(className);
                    rDocument.put(pClass.getSimpleName(), JCasUtil.select(pCas, pClass).size());
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {

            for (Annotation annotation : JCasUtil.select(pCas, Annotation.class)) {
                if (!classNames.contains(annotation.getType().getName())) {
                    rDocument.put(annotation.getType().getShortName(), JCasUtil.select(pCas, annotation.getClass()).size());
                    classNames.add(annotation.getType().getName());
                }
            }
        }

        return rDocument;

    }

    private Document getMetaInformation(JCas pCas) {

        Document rDocument = new Document();

        rDocument.put("size", pCas.size());

        return rDocument;

    }
}
