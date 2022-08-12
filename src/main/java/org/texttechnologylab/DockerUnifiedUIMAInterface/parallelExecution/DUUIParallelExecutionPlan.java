package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelExecution;

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

    @Override
    public IDUUIExecutionPlan copy() {
        return this;
    }

    @Override
    public Future<IDUUIExecutionPlan> awaitMerge() {
        Executor executor = Runnable::run;
        FutureTask<IDUUIExecutionPlan> future =
                new FutureTask<>(() -> {
                    this.merge();
                    return this;
                });
        executor.execute(future);
        return future;
    }

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

    protected void setJCas(JCas jCas){
        this.jCas=jCas;
    }

    private void merge() {
        JCas jCas = previous.get(0).getJCas();
        for (int i = 1; i < previous.size(); i++) {
            MergerFunctions.mergeAll(previous.get(i).getJCas(), jCas);
        }
        this.jCas = jCas;
    }

}
