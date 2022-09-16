package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DUUIParallelExecutionPlan implements IDUUIExecutionPlan {

    private static final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final DUUIComposer.PipelinePart pipelinePart;
    private JCas jCas;
    private final List<DUUIParallelExecutionPlan> previous = new ArrayList<>();

    private final List<DUUIParallelExecutionPlan> next = new ArrayList<>();

    private final AtomicBoolean merged = new AtomicBoolean(false);
    private FutureTask<IDUUIExecutionPlan> future;

    //cache
    private List<String> inputs;
    private List<String> outputs;

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart, JCas jcas) {
        this.pipelinePart = pipelinePart;
        this.jCas = jcas;
    }

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart) {
        this(pipelinePart, null);
    }

    @Override
    public List<IDUUIExecutionPlan> getNextExecutionPlans() {
        return new ArrayList<>(next);
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
    public synchronized Future<IDUUIExecutionPlan> awaitMerge() {
        if (!merged.getAndSet(true)) {
            future = new FutureTask<>(() -> {
                this.merge();
                return this;
            });
            executor.execute(future);
        }
        return future;
    }

    /**
     * @return returns the jCas. May return null if not set.
     */
    @Override
    public JCas getJCas() {
        return jCas;
    }

    public List<DUUIParallelExecutionPlan> getPrevious() {
        return previous;
    }

    public List<DUUIParallelExecutionPlan> getNext() {
        return next;
    }

    @Override
    public DUUIComposer.PipelinePart getPipelinePart() {
        return pipelinePart;
    }

    protected void addNext(DUUIParallelExecutionPlan parallelExecutionPlan) {
        next.add(parallelExecutionPlan);
    }

    protected void addPrevious(DUUIParallelExecutionPlan parallelExecutionPlan) {
        previous.add(parallelExecutionPlan);
    }

    /**
     * Creates a new CAS and merges all previous CASes.
     * Can handle null values from previous.
     * Can handle no previous.
     */
    //* Reuses first previous JCas if exists.
    private void merge() {
        if (jCas != null)
            return;
        try {
            jCas = JCasFactory.createJCas();
        } catch (UIMAException e) {
            throw new RuntimeException(e);
        }
        for (IDUUIExecutionPlan iduuiExecutionPlan : previous) {
            JCas previousJCas;
            try {
                previousJCas = iduuiExecutionPlan.awaitMerge().get().getJCas();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            if (previousJCas != null) {
                if (jCas == null) {
                    jCas = previousJCas;
                } else {
                    MergerFunctions.mergeAll(previousJCas, jCas);
                }
            }
        }
    }

    public List<String> getInputs(){
        if(pipelinePart!=null && inputs==null) {
            try {
                inputs = pipelinePart.getDriver().getInputsOutputs(pipelinePart.getUUID()).getInputs();
            } catch (ResourceInitializationException e) {
                throw new RuntimeException(e);
            }
        }
        return inputs;
    }

    public List<String> getOutputs(){
        if(pipelinePart!=null && outputs==null) {
            try {
                outputs = pipelinePart.getDriver().getInputsOutputs(pipelinePart.getUUID()).getOutputs();
            } catch (ResourceInitializationException e) {
                throw new RuntimeException(e);
            }
        }
        return outputs;
    }


}
