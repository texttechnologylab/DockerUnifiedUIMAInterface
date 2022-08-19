package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelExecution;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class DUUIParallelExecutionPlan implements IDUUIExecutionPlan {

    private static final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final DUUIComposer.PipelinePart pipelinePart;
    private JCas jCas;
    private final List<IDUUIExecutionPlan> previous = new ArrayList<>();
    private final List<IDUUIExecutionPlan> next = new ArrayList<>();

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart, JCas jcas) {
        this.pipelinePart = pipelinePart;
        this.jCas = jcas;
    }

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart) {
        this(pipelinePart,null);
    }

    @Override
    public List<IDUUIExecutionPlan> getNextExecutionPlans() {
        return next;
    }

    /**
     * copy complete graph
     */
    @Override
    public IDUUIExecutionPlan copy() {
        //TODO
        return this;
    }

    @Override
    public Future<IDUUIExecutionPlan> awaitMerge() {
        FutureTask<IDUUIExecutionPlan> future = new FutureTask<>(() -> {
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

    protected void addNext(DUUIParallelExecutionPlan parallelExecutionPlan) {
        next.add(parallelExecutionPlan);
    }

    protected void addPrevious(IDUUIExecutionPlan iduuiExecutionPlan) {
        previous.add(iduuiExecutionPlan);
    }

    /**
     * Sets a merged jCas from previous.
     * Reuses first previous JCas if exists.
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
