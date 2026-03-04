package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.jcas.JCas;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Interface for execution plans of the DUUI composer pipleine.
 */
public interface IDUUIExecutionPlan {
    List<IDUUIExecutionPlan> getNextExecutionPlans();

    Future<IDUUIExecutionPlan> awaitMerge();

    JCas getJCas();

    DUUIComposer.PipelinePart getPipelinePart();
}
