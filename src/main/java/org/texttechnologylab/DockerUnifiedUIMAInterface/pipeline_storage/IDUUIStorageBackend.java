package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Vector;

public interface IDUUIStorageBackend {
    public void addNewRun(String name, DUUIComposer composer) throws SQLException;
    public IDUUIPipelineComponent loadComponent(String id);
    public void addMetricsForDocument(DUUIPipelineDocumentPerformance perf);
    public void finalizeRun(String name, Instant start, Instant end) throws SQLException;
    public void shutdown() throws UnknownHostException;

    /**
     * Whether the storage backend should track error documents.
     * @return true if error documents should be tracked, false otherwise
     */
    boolean shouldTrackErrorDocs();
}
