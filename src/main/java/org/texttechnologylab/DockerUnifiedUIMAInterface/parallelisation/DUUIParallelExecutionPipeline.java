package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.javatuples.Pair;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIWorker.ComponentLock;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

public class DUUIParallelExecutionPipeline extends DirectedAcyclicGraph<String, CustomEdge>  {

    /*
     * 
     * Taken and adapted from: https://jvmaware.com/priority-queue-and-threadpool/
     */
    static class ComparableFutureTask<T> extends FutureTask<T> implements Comparable<ComparableFutureTask<T>>{

        private final DUUIWorker _task;
        public ComparableFutureTask(Callable<T> task) {
            super(task);
            _task = (DUUIWorker) task;
        }

        @Override
        public int compareTo(ComparableFutureTask<T> that) {
            return Integer.compare(_task.getPriority(), that._task.getPriority());
        }
    }

    static class ParallelExecutor extends ThreadPoolExecutor {

        final Map<String, Set<String>> _childMap;
        final Map<String, Callable<Pair<String, Set<String>>>> _componentRunners;
        final Set<String> _submitted = new HashSet<>(100);

        public ParallelExecutor(int corePoolSize, int maximumPoolSize, Map<String, Callable<Pair<String, Set<String>>>> componentRunners) {
            super(corePoolSize, maximumPoolSize, 0L, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<Runnable>());
            _childMap = new HashMap<>();
            _componentRunners = componentRunners;
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new ComparableFutureTask<>(callable);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            if (t == null && r instanceof Future<?>) {
                Object result = null;
                try {
                    result = ((Future<?>) r).get();
                    Pair<String, Set<String>> next = (Pair<String, Set<String>>) result; 
                    for (String child : next.getValue1()) {
                        if (_submitted.contains(next.getValue0()+child))
                            continue;

                        Callable<?> childRunner = _componentRunners.get(next.getValue0()+child);
                        this.submit(childRunner);
                    }
                } catch (CancellationException ce) {
                    t = ce;
                } catch (ExecutionException ee) {
                    t = ee.getCause();
                } catch (InterruptedException ie) {
                    // If future got interrupted exception, we want to interrupt parent thread itself.
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.out.printf("RESULT NOT CONVERTIBLE: %s%n Exception: %s, %s ", result, e, e.getMessage());
                }
            }
        }

    }

    public final Map<String, PipelinePart> _pipeline;
    public final Iterable<String> _executionplan;  
    final Map<String, Callable<Pair<String, Set<String>>>> _componentRunners = new ConcurrentHashMap<>(100); 
    List<Future<Pair<String, Set<String>>>> runningComponents = new ArrayList<>();
    final ThreadPoolExecutor _executor;
    final Set<String> _topNodes = new HashSet<>();

    public DUUIParallelExecutionPipeline(Vector<PipelinePart> flow) {
        super(CustomEdge.class);
        _pipeline = flow.stream()
        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));
                            
        initialiseDAG();
        _executionplan = () -> iterator();
        _executor = new ParallelExecutor(10, 10, _componentRunners);

        for (String node : _executionplan) {
            if (getAncestors(node).size() == 0)
                _topNodes.add(node);
        }
    }
    

    public Map<String, Set<String>> getGraph() {
        Map<String, Set<String>> graph = new LinkedHashMap<>(_pipeline.size());

        for (String parent : _executionplan) {
            graph.put(
                _pipeline.get(parent).getSignature().toString(), 
                getChildren(parent)
                    .stream()
                    .map(c -> _pipeline.get(c).getSignature().toString())
                    .collect(Collectors.toSet())
            );
        }
        return graph; 
    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        
        Future<Pair<String, Set<String>>> future = null;
        for (String node : _topNodes) {
            Callable<Pair<String, Set<String>>> runner = getComponentRunner(name, node, jc, perf);
            future = _executor.submit(runner);
            runningComponents.add(future);
        }
    }

    public void shutdown() throws Exception {
        try {
            while (!_executor.getQueue().isEmpty()) {
                Thread.sleep(3000);
                System.out.printf("Awaiting Threadpool! Queue size %d: | Completed Tasks: %d %n", 
                    _executor.getQueue().size(), _executor.getCompletedTaskCount());
            }

            _executor.shutdown();
            while (_executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {}
        } catch (Exception e) {
            _executor.shutdownNow();
            throw e; 
        }
    }

    public List<Callable<Pair<String, Set<String>>>> getAllComponentRunners(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {

        List<Callable<Pair<String, Set<String>>>> compRunners = new ArrayList<>(_pipeline.size()); 

        for (String compId : _pipeline.keySet()) {
            compRunners.add(getComponentRunner(name, compId, jc, perf));
        }
        
        return compRunners; 
    }

    public Callable<Pair<String, Set<String>>> getComponentRunner(String name, String uuid) {
        if (!_componentRunners.containsKey(name+uuid)) return null;

        return _componentRunners.get(name+uuid);
    }

    public Callable<Pair<String, Set<String>>> getComponentRunner(String name, String uuid, JCas jc, DUUIPipelineDocumentPerformance perf) {
        if (_componentRunners.containsKey(name+uuid)) 
            return _componentRunners.get(name+uuid);

        initialiseComponentRunners(name, jc, perf);

        return _componentRunners.get(name+uuid);
    }

    public synchronized void initialiseComponentRunners(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        Map<String, Pair<Collection<ComponentLock>, Collection<ComponentLock>>> _locks = initialiseLocks(jc);
        String _title = JCasUtil.select(jc, DocumentMetaData.class)
                .stream().map(meta -> meta.getDocumentTitle()).findFirst().orElseGet(() -> "");

        DUUIPipelineProfiler.documentMetaDataUpdate(name, _title, jc.size());
        
        _pipeline.forEach((uuid, comp) -> 
            {
                Collection<ComponentLock> selfLocks = _locks.get(uuid).getValue0();
                Collection<ComponentLock> childLocks = _locks.get(uuid).getValue1();


                _componentRunners.put(name+uuid, new DUUIWorker(name, comp, jc, perf, selfLocks, childLocks, getChildren(uuid), getHeight(uuid)));
            });
    }

    private Map<String, Pair<Collection<ComponentLock>, Collection<ComponentLock>>> initialiseLocks(JCas jc) {
        
        Map<String, Map<Class<? extends Annotation>, ComponentLock>> 
            latches = new ConcurrentHashMap<>(_pipeline.size()); 
        Map<String, Map<Class<? extends Annotation>, AtomicInteger>> 
            latchesCovered = new ConcurrentHashMap<>(_pipeline.size()); 
        Map<String, List<ComponentLock>> childLatches = new HashMap<>(_pipeline.size()); 

        // for every node save a lock for every dependency if not already in jc 
        for (PipelinePart part : _pipeline.values()) {
            String self = part.getUUID();
            List<Class<? extends Annotation>> dependencies = part.getSignature().getInputs();
            Map<Class<? extends Annotation>, ComponentLock> selfLatches = new HashMap<>(dependencies.size());
            Map<Class<? extends Annotation>, AtomicInteger> selfLatchesCovered = new HashMap<>(dependencies.size());

            for (Class<? extends Annotation> dependency : dependencies) {
                // TODO: Add customizable type-checking e.g. Feature language of DocumentMetaData Annotation
                if (JCasUtil.select(jc, dependency).isEmpty()) { 
                    selfLatches.put(dependency, new ComponentLock(1));
                    selfLatchesCovered.put(dependency, new AtomicInteger(1));
                }
            }

            latches.put(self, selfLatches);
            latchesCovered.put(self, selfLatchesCovered);
        }

        // for every child save every lock which corresponds to a provided output
        for (PipelinePart part : _pipeline.values()) {
            String self = part.getUUID();
            List<Class<? extends Annotation>> outputs = part.getSignature().getOutputs();
            List<ComponentLock> childLocks =  new ArrayList<>();
            for (String child : getChildren(self)) {
                for (Class<? extends Annotation> dependency : latches.get(child).keySet()) {
                    if (outputs.contains(dependency)) {
                        childLocks.add(latches.get(child).get(dependency));
                        latchesCovered.get(child).get(dependency).decrementAndGet();
                    }
                }
            }
            childLatches.put(self, childLocks);
        }

        // check that for every component every input is provided
        if (!latchesCovered.values().stream()
        .map(map -> map.values())
        .filter(atomintlist -> atomintlist.size() > 0)
        .map(atomintlist -> atomintlist.stream().mapToInt(atomint -> atomint.get()))
        .allMatch(intlist -> intlist.max().getAsInt() < 1))
            throw new RuntimeException(
                "[ExecutionPipeline] There are pipeline components who will never get their inputs!");

        return latches.entrySet().stream()
            .map(entry -> 
            {
                String self = entry.getKey();
                Collection<ComponentLock> selfLocks = entry.getValue().values(); 
                Collection<ComponentLock> childLocks = childLatches.get(self);
                return Pair.with(self, Pair.with(selfLocks, childLocks));
            }).collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));
        
    }

    public Set<String> getChildren(String child) {
        return outgoingEdgesOf(child).stream()
            .map(CustomEdge::getTarget)
            .collect(Collectors.toSet());
    }

    public int getHeight(String node) {
        if (incomingEdgesOf(node).isEmpty())
            return 1;

        return 1 + incomingEdgesOf(node).stream()
            .map(CustomEdge::getSource)
            .mapToInt(this::getHeight)
            .max().orElse(0);

    }

    private void initialiseDAG() {

        for (PipelinePart part1 : _pipeline.values()) {
            this.addVertex(part1.getUUID());

            for (PipelinePart part2 : _pipeline.values()) {
                if (part1.getUUID() == part2.getUUID()) continue;

                this.addVertex(part2.getUUID());

                try {

                    switch (part1.getSignature().compare(part2.getSignature())) {
                        // Direct Dependency
                        case 1:
                            this.addEdge(part1.getUUID(), part2.getUUID());
                            break;
                        case -1: 
                            this.addEdge(part2.getUUID(), part1.getUUID());
                            break;
                        // Cycle
                        case -2:
                            break;
                        // No-Dependency
                        default:
                            break;
                        
                    }
                } catch (GraphCycleProhibitedException e) {
                    System.out.println("[DUUIExecutionPipeline] There is a circular dependency between the pipeline components. "
                        + "Parallel execution is impossible!");
                    throw e;
                }
            }
        }
    }  

}

class CustomEdge extends DefaultEdge {
    public String getSource() {
        return super.getSource().toString();
    }

    public String getTarget() {
        return super.getTarget().toString();
    }
}
