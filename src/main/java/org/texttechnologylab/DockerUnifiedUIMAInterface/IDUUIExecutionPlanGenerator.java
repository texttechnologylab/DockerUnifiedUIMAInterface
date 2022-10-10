package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.jcas.JCas;

public interface IDUUIExecutionPlanGenerator {
    public IDUUIExecutionPlan generate(JCas jc);
}
