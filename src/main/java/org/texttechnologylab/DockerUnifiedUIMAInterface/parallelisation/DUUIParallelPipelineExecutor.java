package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.util.function.Predicate.not;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.javatuples.Pair;
import org.texttechnologylab.DockerUnifiedUIMAInterface.AnnotatorUnreachableException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceStatistics;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIWorker.ComponentLock;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

public class DUUIParallelPipelineExecutor extends DUUILinearPipelineExecutor implements IDUUIResource {

    static interface PipelineWorker {

        int getPriority();
    }

    class JCasCompletionWorker implements PipelineWorker, Callable<DUUIWorker> {

        final JCas jc;
        final int parentLevel;

        public JCasCompletionWorker(JCas jc, int parentLevel) {
            this.jc = jc;
            this.parentLevel = parentLevel;
        }

        public DUUIWorker call() {
            DUUIComposer.Config.write(jc);
            ResourceManager.getInstance().returnCas(this.jc);
            return null; 
        }

        public int getPriority() {
            return _executor._currentLevel.get() == parentLevel ? 
                parentLevel + 1 : Integer.MIN_VALUE;
        }
    }

    /*
     * 
     * Taken and adapted from: https://jvmaware.com/priority-queue-and-threadpool/
     */
    static class ComparableFutureTask<T> extends FutureTask<T> implements Comparable<ComparableFutureTask<T>> {

        private final PipelineWorker _task;
        public ComparableFutureTask(Callable<T> task) {
            super(task);
            _task = (PipelineWorker) task;
        }

        @Override
        public int compareTo(ComparableFutureTask<T> that) {
            return Integer.compare(_task.getPriority(), that._task.getPriority());
        }
    }

    /*  
     * 
     * Taken and adapted from: https://stackoverflow.com/questions/19528304/how-to-get-the-threadpoolexecutor-to-increase-threads-to-max-before-queueing
     */
    class PipelineQueue extends PriorityBlockingQueue<Runnable> {

        static final long serialVersionUID = -6903933921423432194L;

        @Override
        public boolean offer(Runnable e) {
            if (_executor.getPoolSize() >= Config.strategy().getMaxPoolSize()) {
                return super.offer(e);
            } else {
                System.out.printf("[ParallelPipeline] NEW THREAD Pool: %d | Max: %d%n", 
                    _executor.getPoolSize(), Config.strategy().getMaxPoolSize());
                return false;
            }
        }
    }

    class PipelineExecutor extends ThreadPoolExecutor {

        final Map<String, Set<String>> _childMap;
        final Map<String, Callable<DUUIWorker>> _componentRunners;
        final Set<String> _submitted = new HashSet<>(100);
        final Map<String, AtomicInteger> _finishedComponents; 
        final AtomicInteger _documentProgress;
        final AtomicInteger _currentLevel = new AtomicInteger(1);
        final HashSet<Callable<?>> _failedWorkers = new HashSet<>(100);
        final BlockingQueue<Callable<DUUIWorker>> _backup = new LinkedBlockingQueue<>();

        public PipelineExecutor(int corePoolSize, 
                                int maximumPoolSize, 
                                Map<String, 
                                Callable<DUUIWorker>> componentRunners, 
                                Map<String, 
                                AtomicInteger> finishedComponents, 
                                AtomicInteger documentProgress) {

            super(corePoolSize, 
                maximumPoolSize, 
                DUUIComposer.Config.strategy().getTimeout(TimeUnit.MILLISECONDS), 
                TimeUnit.MILLISECONDS, 
                new PipelineQueue()
            );
            _childMap = new HashMap<>();
            _componentRunners = componentRunners;
            _finishedComponents = finishedComponents;
            _documentProgress = documentProgress;

            this.setRejectedExecutionHandler((Runnable r, ThreadPoolExecutor executor) -> {
                // This does the actual put into the queue. Once the max threads
                //  have been reached, the tasks will then queue up.
                System.out.println("NEW TASK " + r);
                executor.getQueue().add(r);
                // we do this after the put() to stop race conditions
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException(
                        "Task " + r + " rejected from " + executor);
                }
            });
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new ComparableFutureTask<>(callable);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            if (r instanceof Future<?>) {
                Object result = null;
                try {
                    result = ((Future<?>) r).get();
                    if (result == null) return;

                    if (result instanceof DUUIWorker) {
                        DUUIWorker worker = (DUUIWorker) result; 
                        afterWorker(worker);
                    }
                } catch (ExecutionException ee) {
                    t = ee.getCause();
                    if (t instanceof AnnotatorUnreachableException) {
                        AnnotatorUnreachableException e = 
                            (AnnotatorUnreachableException) t;
                        this.submit(e.failedWorker);
                        _failedWorkers.add(e.failedWorker);
                    }
                } catch (InterruptedException | CancellationException ie) {
                    Thread.currentThread().interrupt();
                } catch (RejectedExecutionException e) {
                    System.out.printf("[DUUIParallelPipeline] Worker rejected: %s %n", result);
                    e.printStackTrace();
                }

                
            }
        }

        void afterWorker(DUUIWorker worker) throws InterruptedException {
            // Scale component down if all instances 
            // of current component-batch are finished
            _finishedComponents.get(worker._component.getUUID()).incrementAndGet(); 
            _currentLevel.set(worker._height);
                        
            // Schedule children
            Set<String> children = worker._childrenIds;
            children.stream()
                .map(child -> worker._name+child)
                .filter(not(_submitted::contains))
                .map(_componentRunners::get)
                .forEach(_backup::add);

            
            // Return JCas to pool if document processing is finished
            boolean isDocumentFinished = 
                worker._jCasLock.await(10, TimeUnit.NANOSECONDS);

            if (isDocumentFinished) {
                this.submit(new JCasCompletionWorker(worker._jc, worker.getPriority()));
            }

            if (getResourceManager().isBatchReadIn()) {
                while (!_backup.isEmpty()) {
                    Callable<?> t = _backup.take();
                    this.submit(t);
                }
            }   
        }

    }

    final PipelineExecutor _executor;
    final Set<String> _topNodes = new HashSet<>();
    final Map<String, Callable<DUUIWorker>> _componentRunners = new ConcurrentHashMap<>(100); 
    final Map<String, AtomicInteger> _completedComponentInstances; 
    final AtomicInteger _registeredDocumentsCount = new AtomicInteger(0);
    final Map<String, Integer> _heightMap;

    final Map<String, Object> _collectionMap = new ConcurrentHashMap<>(3);

    public DUUIParallelPipelineExecutor(Vector<PipelinePart> flow) {
        super(flow);

        ResourceManager.register(this);

        _completedComponentInstances = new ConcurrentHashMap<>(_pipeline.size()); 
        _heightMap = new ConcurrentHashMap<>(_pipeline.size()); 
        
        for (String node : _executionplan) {
            _completedComponentInstances.put(node, new AtomicInteger(0));
            _heightMap.put(node, getHeight(node));
            if (getAncestors(node).size() == 0)
            _topNodes.add(node);
        }

        _executor = new PipelineExecutor(Config.strategy().getCorePoolSize(),
             Config.strategy().getMaxPoolSize(), 
             _componentRunners, 
             _completedComponentInstances, 
             _registeredDocumentsCount
        );
    }

    @Override
    public void scale(ResourceStatistics statistics) {

        if (getResourceManager().isBatchReadIn()) {
            while (!_executor._backup.isEmpty()) {
                try {
                    Callable<?> t = _executor._backup.take();
                    _executor.submit(t);
                } catch (InterruptedException e) {
                }
            }
        }  
        // int poolSize = statistics.getHostUsageStatistics()
        //     .calculateDynamicPoolsize();

        // if (DUUIComposer.Config.strategy() instanceof AdaptiveStrategy) {
        //     if (_executor.getMaximumPoolSize() != poolSize) {
        //         System.out.printf("[%s][ParallelExecutor] Scaling up to: %d %n", 
        //             Thread.currentThread().getName(), poolSize);
        //         ((AdaptiveStrategy) DUUIComposer.Config.strategy()).setMaxPoolSize(poolSize);
        //         _executor.setMaximumPoolSize(poolSize);
        //     }
        // }

    }

    @Override
    public Map<String, Object> collect() {
        _collectionMap.put("currentLevel", _executor._currentLevel);
        _collectionMap.put("registeredDocumentsCount", _registeredDocumentsCount);
        _collectionMap.put("completedComponentInstances", _completedComponentInstances);
        _collectionMap.put("heightMap", _heightMap);

        return _collectionMap; 
    }
    
    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
         
        _registeredDocumentsCount.incrementAndGet();

        // Future<DUUIWorker> future = null;
        for (String node : _topNodes) {
            Callable<DUUIWorker> runner = getComponentRunner(name, node, jc, perf);
            _executor.submit(runner);
            // runningComponents.add(future);
        }
    }

    public void shutdown() throws Exception {
        getResourceManager().setBatchReadIn(true);
        try {
            while (!_executor.getQueue().isEmpty()) {
                Thread.sleep(3000);
                // System.out.printf("Awaiting Threadpool! Queue size %d: | Completed Tasks: %d %n", 
                //     _executor.getQueue().size(), _executor.getCompletedTaskCount());
            }

            _executor.shutdown();
            while (_executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {}
        } catch (Exception e) {
            _executor.shutdownNow();
            throw e; 
        }
    }

    public void destroy() {
        _executor.shutdownNow();
    }

    public Callable<DUUIWorker> getComponentRunner(String name, String uuid, JCas jc, DUUIPipelineDocumentPerformance perf) {
        if (_componentRunners.containsKey(name+uuid)) 
            return _componentRunners.get(name+uuid);

        initialiseComponentRunners(name, jc, perf);

        return _componentRunners.get(name+uuid);
    }

    public void initialiseComponentRunners(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        Map<String, Pair<Collection<ComponentLock>, Collection<ComponentLock>>> _locks = initialiseLocks(jc);
        String _title = JCasUtil.select(jc, DocumentMetaData.class)
                .stream().map(meta -> meta.getDocumentTitle()).findFirst().orElseGet(() -> "");

        DUUIPipelineProfiler.documentMetaDataUpdate(name, _title, jc.size());

        final CountDownLatch jCasLock = new CountDownLatch(_pipeline.size());

        _pipeline.forEach((uuid, comp) -> 
            {
                Collection<ComponentLock> selfLocks = _locks.get(uuid).getValue0();
                Collection<ComponentLock> childLocks = _locks.get(uuid).getValue1();


                _componentRunners.put(name+uuid, new DUUIWorker(name, comp, jc, perf, selfLocks, childLocks, getChildren(uuid), getHeight(uuid), jCasLock));
            });
    }

    private Map<String, Pair<Collection<ComponentLock>, Collection<ComponentLock>>> initialiseLocks(JCas jc) {
        
        Map<String, Map<Class<? extends Annotation>, ComponentLock>> 
            latches = new ConcurrentHashMap<>(_pipeline.size()); 
        Map<String, Map<Class<? extends Annotation>, AtomicInteger>> 
            latchesCovered = new ConcurrentHashMap<>(_pipeline.size()); 
        Map<String, List<ComponentLock>> childLatches = new HashMap<>(_pipeline.size()); 

        // for every node save a lock for every dependency if not already in JCas 
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

}