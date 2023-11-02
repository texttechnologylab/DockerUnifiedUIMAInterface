package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import java.util.Map;
import java.util.Set;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

/**
 * Interface for executors who are responsible for scheduling the processing of documents.
 * 
 */
public interface IDUUIPipelineExecutor {

    /**
     * Pass a document to schedule for processing.
     * 
     * @param name        Name of the run. Has to be unique for a document.
     * @param jc          CAS containing document.
     * @param perf        Data-structure storing performance metrics.
     * @throws Exception  Exceptions propagated through by {@link PipelinePart.run}
     */
    void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception;


    /**
     * Shuts down the pipeline and stops scheduling any more tasks.
     * 
     * @throws Exception Exceptions associated with a failed shutdown.
     */
    default void shutdown() throws Exception {
    };

    /**
     * Shuts down and clears the pipeline without a grace-period in-case of failures.
     * 
     */
    default void destroy() {
    }

    /**
     * Representation of the pipeline-graph as a {@link java.util.Map} mapping a parent-node 
     * to its child-nodes.
     * 
     * @return Graph.
     */
    Map<String, Set<String>> getGraph();

}
