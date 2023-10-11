package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class DUUIMockStorageBackend implements IDUUIStorageBackend {
    private HashSet<String> _runs;
    private HashMap<String,Vector<DUUIPipelineDocumentPerformance>> _performance;

    public DUUIMockStorageBackend() {
        _runs = new HashSet<>();
        _performance = new HashMap<>();
    }

    public HashSet<String> getRunMap() {
        return _runs;
    }

    public HashMap<String,Vector<DUUIPipelineDocumentPerformance>> getPerformanceMonitoring() {
        return _performance;
    }

    public void addNewRun(String name, DUUIComposer composer) throws SQLException {
        _runs.add(name);
    }

    public IDUUIPipelineComponent loadComponent(String id) {
        return new IDUUIPipelineComponent();
    }

    public void addMetricsForDocument(DUUIPipelineDocumentPerformance perf) {
        Vector<DUUIPipelineDocumentPerformance> vec = _performance.get(perf.getRunKey());
        if(vec == null) {
            vec = new Vector<>();
            vec.add(perf);
            _performance.put(perf.getRunKey(),vec);
        }
        else {
            vec.add(perf);
        }
    }

    public void finalizeRun(String name, Instant start, Instant end) throws SQLException {
    }

    public void shutdown() throws UnknownHostException {

    }

    @Override
    public boolean shouldTrackErrorDocs() {
        System.err.println("WARNING: Mock storage backend does not support error document tracking!");
        return false;
    }
}
