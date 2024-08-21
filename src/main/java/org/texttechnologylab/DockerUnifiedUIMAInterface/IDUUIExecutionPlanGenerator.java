package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.jcas.JCas;

/**
 * Interface for generating execution plans {@link IDUUIExecutionPlan}.
 */
public interface IDUUIExecutionPlanGenerator {
    /**
     * Generates an execution plan for a given JCas.
     * @param jc The JCas to generate the execution plan for.
     * @return The generated execution plan.
     */
    public IDUUIExecutionPlan generate(JCas jc);
}
