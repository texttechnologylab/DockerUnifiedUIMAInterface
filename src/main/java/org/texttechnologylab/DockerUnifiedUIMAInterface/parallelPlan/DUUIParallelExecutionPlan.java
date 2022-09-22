package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;
import org.texttechnologylab.annotation.DocumentModification;

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

    private final CompletableFuture<JCas> isAnnotated;
    private FutureTask<IDUUIExecutionPlan> future;

    // cache
    private List<String> inputs;
    private List<String> outputs;

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart, JCas jcas) {
        this.pipelinePart = pipelinePart;
        this.jCas = jcas;
        isAnnotated = new CompletableFuture<>();

    }

    public DUUIParallelExecutionPlan(DUUIComposer.PipelinePart pipelinePart) {
        this(pipelinePart, null);
    }

    /**
     * @return
     */
    @Override
    public List<IDUUIExecutionPlan> getNextExecutionPlans() {
        return new ArrayList<>(next);
    }

    /**
     * copy complete graph
     */
    @Override
    public IDUUIExecutionPlan copy() {
        // TODO
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

    /**
     *
     * @return the predecessors of this ExecutionPlan
     */
    public List<DUUIParallelExecutionPlan> getPrevious() {
        return previous;
    }

    /**
     *
     * @return the successors of this ExecutionPlan
     */
    public List<DUUIParallelExecutionPlan> getNext() {
        return next;
    }

    /**
     *
     * @return The pipelinePart of this ExecutionPlan.
     */
    @Override
    public synchronized DUUIComposer.PipelinePart getPipelinePart() {
        return pipelinePart;
    }

    /**
     *
     * @param parallelExecutionPlan Adds ExecutionPlan as successor. Used for Graph generation.
     */
    protected void addNext(DUUIParallelExecutionPlan parallelExecutionPlan) {
        next.add(parallelExecutionPlan);
    }

    /**
     *
     * @param parallelExecutionPlan Adds ExecutionPlan as predecessor. Used for Graph generation.
     */
    protected void addPrevious(DUUIParallelExecutionPlan parallelExecutionPlan) {
        previous.add(parallelExecutionPlan);
    }

    /**
     * Creates a new CAS and merges all previous CASes.
     * Can handle null values from previous.
     * Can handle no previous.
     */
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
                previousJCas = iduuiExecutionPlan.awaitAnnotation().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            MergerFunctions.mergeAll(previousJCas, jCas);
        }
        System.out.println("merging... ");
        System.out.println(previous.size());
        System.out.println(getInputs());
        System.out.println(getOutputs());
        System.out.println("Java Document");
        for (DocumentModification t : JCasUtil.select(jCas, DocumentModification.class)) {
            System.out.println(t);
        }
        System.out.println();
    }

    /**
     * @return The inputs of the PipelinePart. Provides caching. Not thread safe.
     */
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

    /**
     *
     * @return The outputs of the PipelinePart. Provides caching. Not thread safe.
     */
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

    /**
     * marks the JCas of this Plan as annotated
     */
    @Override
    public void setAnnotated() {
        isAnnotated.complete(jCas);
    }

    /**
     * @return Future for the annotated JCas
     */
    public Future<JCas> awaitAnnotation(){
        return isAnnotated;
    }
}
