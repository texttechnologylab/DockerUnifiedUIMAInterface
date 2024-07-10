package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.jcas.JCas;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Interface for execution plans of the DUUI composer pipleine.
 */
public interface IDUUIExecutionPlan {
    public List<IDUUIExecutionPlan> getNextExecutionPlans();

    public Future<IDUUIExecutionPlan> awaitMerge();
    public JCas getJCas();
    public DUUIComposer.PipelinePart getPipelinePart();
}
