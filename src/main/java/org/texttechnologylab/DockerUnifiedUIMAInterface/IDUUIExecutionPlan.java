package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.jcas.JCas;

import java.util.List;
import java.util.concurrent.Future;

public interface IDUUIExecutionPlan {
    public List<IDUUIExecutionPlan> getNextExecutionPlans();

    /**
     * not used at the moment
     */
    public IDUUIExecutionPlan copy();
    public Future<IDUUIExecutionPlan> awaitMerge();
    public JCas getJCas();
    public DUUIComposer.PipelinePart getPipelinePart();
    public void setAnnotated();
    public  Future<JCas> awaitAnnotation();
}
