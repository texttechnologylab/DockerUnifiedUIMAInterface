package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelExecution;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DUUIParallelExecutionPlan implements IDUUIExecutionPlan {

    private final DUUIComposer.PipelinePart pipelinePart;
    private JCas jCas;

    private final List<IDUUIExecutionPlan> previous = new ArrayList<>();
    private final List<IDUUIExecutionPlan> next = new ArrayList<>();

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart, JCas jcas) {
        this.pipelinePart = pipelinePart;
        this.jCas = jcas;
    }

    @Override
    public List<IDUUIExecutionPlan> getNextExecutionPlans() {
        return next;
    }

    /**
     * copy complete graph
     * @return
     */
    @Override
    public IDUUIExecutionPlan copy() {
        //TODO
        return this;
    }

    @Override
    public Future<IDUUIExecutionPlan> awaitMerge() {
        // TODO "real" Future
        Executor executor = Runnable::run;
        FutureTask<IDUUIExecutionPlan> future =
                new FutureTask<>(() -> {
                    this.merge();
                    return this;
                });
        executor.execute(future);
        return future;
    }

    /**
     * @return returns the jCas. May return null if not set.
     */
    @Override
    public JCas getJCas() {
        return jCas;
    }

    @Override
    public DUUIComposer.PipelinePart getPipelinePart() {
        return pipelinePart;
    }

    protected boolean addNext(DUUIParallelExecutionPlan parallelExecutionPlan) {
        return next.add(parallelExecutionPlan);
    }

    protected boolean addPrevious(IDUUIExecutionPlan iduuiExecutionPlan) {
        return previous.add(iduuiExecutionPlan);
    }

    protected void setJCas(JCas jCas) {
        this.jCas = jCas;
    }

    /**
     * Sets a merged jCas from previous.
     * Reuses first previous if exists.
     * Can handle null values from previous.
     * Can handle no previous.
     */
    private void merge() {

        jCas = null;
        for (IDUUIExecutionPlan iduuiExecutionPlan : previous) {
            JCas previousJCas = iduuiExecutionPlan.getJCas();
            if (previousJCas != null) {
                if (jCas == null) {
                    jCas = previousJCas;
                } else {
                    MergerFunctions.mergeAll(previousJCas, jCas);
                }
            } else {
                // nothing to merge
            }
        }
        if (jCas == null) {
            try {
                jCas = JCasFactory.createJCas();
            } catch (UIMAException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
