package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import java.util.Map;
import java.util.Set;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

public interface IDUUIPipelineExecutor {

    void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception;

    default void shutdown() throws Exception {
    };

    default void destroy() {
    }

    Map<String, Set<String>> getGraph();;

}
