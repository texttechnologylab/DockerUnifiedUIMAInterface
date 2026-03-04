package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.Instant;

public interface IDUUIStorageBackend {
    void addNewRun(String name, DUUIComposer composer) throws SQLException;

    IDUUIPipelineComponent loadComponent(String id);

    void addMetricsForDocument(DUUIPipelineDocumentPerformance perf);

    void finalizeRun(String name, Instant start, Instant end) throws SQLException;

    void shutdown() throws UnknownHostException;

    /**
     * Whether the storage backend should track error documents.
     *
     * @return true if error documents should be tracked, false otherwise
     */
    boolean shouldTrackErrorDocs();
}
