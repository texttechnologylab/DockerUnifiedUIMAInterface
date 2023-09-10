package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.util.function.Predicate.not;
import static org.junit.jupiter.api.DynamicTest.stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.PipelineProgress;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceViews;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIWorker.ComponentLock;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

public class DUUIParallelPipelineExecutor extends DUUILinearPipelineExecutor implements IDUUIResource {

    /*  
     * 
     * Taken and adapted from: https://stackoverflow.com/questions/19528304/how-to-get-the-threadpoolexecutor-to-increase-threads-to-max-before-queueing
     */
    class PipelineQueue extends PriorityBlockingQueue<Runnable> {

        static final long serialVersionUID = -6903933921423432194L;

        @Override
        public boolean offer(Runnable e) {
            int totalPoolSizes = _executors.stream()
                .mapToInt(PipelineExecutor::getPoolSize)
                .sum();
            if (totalPoolSizes >= Config.strategy().getMaxPoolSize()) {
                return super.offer(e);
            } else {
                // System.out.printf("[ParallelPipeline] NEW THREAD Pool: %d | Max: %d%n", 
                //     _executors.get(_component).getPoolSize(), Config.strategy().getMaxPoolSize());
                return false;
            }
        }
    }

    class PipelineExecutor extends ThreadPoolExecutor {
        final BlockingQueue<DUUIWorker> _backedUp = new LinkedBlockingQueue<>();

        public PipelineExecutor(int corePoolSize, int maximumPoolSize) {
            super(corePoolSize, 
                maximumPoolSize, 
                DUUIComposer.Config.strategy().getTimeout(TimeUnit.MILLISECONDS), 
                TimeUnit.MILLISECONDS, 
                new PipelineQueue()
            );

            this.setRejectedExecutionHandler((Runnable r, ThreadPoolExecutor executor) -> {
                executor.getQueue().add(r);
                if (executor.isShutdown())
                    throw new RejectedExecutionException("Task " + r + " rejected from " + executor);
            });
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new ComparableFutureTask<>(callable);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            if (!(r instanceof Future<?>)) return;

            Object result = null;
            try {
                result = ((Future<?>) r).get();
                if (result == null || ! (result instanceof DUUIWorker)) 
                    return;

                DUUIWorker worker = (DUUIWorker) result; 
                _components.get(worker.component()).complete(worker);
                _currentLevel.set(worker.getPriority());
                _view.acceleration();

                tryFinalizeCas(worker);
                worker._childrenIds.forEach(child -> 
                {
                    final PipelineComponent component = _components.get(child);
                    final PipelineExecutor executor = component._pipelineExecutor;
                    executor.submit(worker._name, component);
                });
            } catch (ExecutionException ee) {
                t = ee.getCause();
                if (t instanceof AnnotatorUnreachableException) {
                    t.printStackTrace();
                    AnnotatorUnreachableException e = (AnnotatorUnreachableException) t;
                    final String workerId = e.failedWorker._name + e.failedWorker.component();
                    if (_failedWorkers.contains(workerId) || !e.resuable) {
                        try {
                            _components.get(e.failedWorker.component()).complete(e.failedWorker);
                            tryFinalizeCas(e.failedWorker);
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                            e1.printStackTrace();
                        }
                    } else {
                        submit(e.failedWorker);
                        _failedWorkers.add(workerId);
                    }
                }
            } catch (InterruptedException | CancellationException ie) {
                Thread.currentThread().interrupt();
            } catch (RejectedExecutionException e) {
                System.out.printf("[DUUIParallelPipeline] Worker rejected: %s %n", result);
                e.printStackTrace();
            }   
        }

        void tryFinalizeCas(DUUIWorker worker) throws InterruptedException {
            worker._jCasLock.countDown();
            final long count = worker._jCasLock.getCount();
            if (worker._jCasLock.await(1, TimeUnit.NANOSECONDS) && count == 0L) {
                JCasCompletionWorker w  = 
                    new JCasCompletionWorker(worker._jc, worker.getPriority());
                if (_levelSynchronized) {
                    _backedUpJcas.add(w);
                    final boolean completed = _comps.stream()
                        .allMatch(PipelineComponent::isCompleted);
                    if (completed) {
                        while ((w = _backedUpJcas.poll()) !=  null) {
                            submit(w);
                        }
                        submit(new ResetterWorker());
                    }
                } else w.call();
            }
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            if (task instanceof DUUIWorker) {
                DUUIWorker worker = (DUUIWorker) task;
                final String workerId = worker._name + worker.component();
                final PipelineComponent component = _components.get(worker.component());
                if (! _failedWorkers.contains(workerId)) component.submit();
            }
            return super.submit(task);
        }

        public void submit(String document, PipelineComponent component) {
            final DUUIWorker worker = component.getTask(document);
            if (worker == null) return;

            if (_levelSynchronized) {
                _backedUp.add(worker);
                if (getResourceManager().isBatchReadIn() && component.canSubmit()) {
                    DUUIWorker w = null;
                    while ((w = _backedUp.poll()) !=  null) {    
                        submit(w);
                    } 
                }
            } else submit(worker);
        }
    }
    
    final Map<String, PipelineComponent> _components;
    final Set<PipelineExecutor> _executors;        
    final Collection<PipelineComponent> _comps;
    
    final AtomicInteger _registeredDocuments = new AtomicInteger(0);
    final BlockingQueue<JCasCompletionWorker> _backedUpJcas = new LinkedBlockingQueue<>();
    final HashSet<String> _failedWorkers = new HashSet<>();
    boolean _levelSynchronized = false;
    final AtomicLong _remainingDuration = new AtomicLong(Long.MAX_VALUE);

    final AtomicInteger _currentLevel = new AtomicInteger(1);
    final Set<String> _topNodes = new HashSet<>();
    final int _pipelineDepth;

    final PipelineProgressView _view;

    public DUUIParallelPipelineExecutor(Vector<PipelinePart> flow) {
        this(flow, false);
    }

    public DUUIParallelPipelineExecutor(Vector<PipelinePart> flow, boolean shared) {
        super(flow);

        ResourceManager.register(this);

        _components = new ConcurrentHashMap<>(_pipeline.size()); 

        final int corePoolSize = Config.strategy().getCorePoolSize();
        final int maxPoolSize = Config.strategy().getMaxPoolSize();
        
        final PipelineExecutor sharedExecutor = !shared ? null : new PipelineExecutor(corePoolSize, maxPoolSize);

        for (String node : _executionplan) {
            final PipelineExecutor executor = !shared ? new PipelineExecutor(corePoolSize, maxPoolSize) : sharedExecutor;
            _components.put(node, new PipelineComponent(node, executor, getHeight(node), getAncestors(node)));
            
            if (getAncestors(node).size() == 0) _topNodes.add(node);
        }
        
        _comps = _components.values();
        _executors = _comps.stream()
            .map(x -> x._pipelineExecutor)
            .distinct()
            .collect(Collectors.toSet());
        
        _pipelineDepth = _comps.stream()
            .map(x -> x._height)
            .max(Integer::compare)
            .orElse(1);

        _view = new PipelineProgressView();
    }

    public DUUIParallelPipelineExecutor withLevelSynchronization(boolean synchronize) { 
        _levelSynchronized = synchronize;
        return this;
    }

    Integer updateLevel() {
        return _executors.stream()
            .map(PipelineExecutor::getQueue)
            .map(BlockingQueue::peek)
            .filter(Predicate.not(Predicate.isEqual(null)))
            .map(ComparableFutureTask.class::cast)
            .mapToInt(ComparableFutureTask::getPriority)
            .max().orElse(1);
    }

    void remainingDuration() {
        final int level = _currentLevel.get();

        final long r = _comps.stream()
            .filter(c -> c._height == level)
            .map(c -> c.remainingDuration())
            .max(Long::compare).orElse(0l);

        _remainingDuration.set(r);
    }

    @Override
    public void scale(ResourceViews statistics) {
        _currentLevel.set(updateLevel());
        // remainingDuration();
        // System.out.printf("[LEVEL-%s] SCALE UP REMAINING TIME MS: %d \n", 
        //     _currentLevel.get(), Duration.ofNanos(_remainingDuration.get()).toMillis());
        // System.out.printf("[LEVEL-%s] READ IN DOCUMENTS: %d \n", 
        //     _currentLevel.get(), _registeredDocuments.get());
    }

    @Override
    public PipelineProgress collect() {
        return _view;
    }
    
    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
         
        _registeredDocuments.incrementAndGet();
        for (PipelineComponent component : _comps.stream().filter(p -> p._ancestors.size() == 0).collect(Collectors.toList())) {
            // final PipelineComponent c = _components.get(component);
            final DUUIWorker runner = getComponentRunner(name, component._uuid, jc, perf);
            component._pipelineExecutor.submit(runner);
        }
    }

    public void shutdown() throws Exception {
        if (_levelSynchronized) getResourceManager().setBatchReadIn(true);

        while (!_comps.stream().allMatch(PipelineComponent::isCompleted) || Thread.interrupted()) {
            Thread.sleep(500);
            // System.out.printf("Awaiting Threadpool! Queue size %d: | Completed Tasks: %d %n", 
            //     _executor.getQueue().size(), _executor.getCompletedTaskCount());
        }

        destroy();
    }

    public void destroy() {
        _executors.forEach(PipelineExecutor::shutdownNow);
    }

    public DUUIWorker getComponentRunner(String name, String uuid, JCas jc, DUUIPipelineDocumentPerformance perf) {
        DUUIWorker w = null;
        if ((w = _components.get(uuid).getTask(name)) != null)
           return w;

        initialiseComponentRunners(name, jc, perf);

        return _components.get(uuid).getTask(name);
    }

    public void initialiseComponentRunners(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        Map<String, Pair<Collection<ComponentLock>, Collection<ComponentLock>>> _locks = initialiseLocks(jc);
        String _title = JCasUtil.select(jc, DocumentMetaData.class)
                .stream().map(meta -> meta.getDocumentTitle()).findFirst().orElseGet(() -> name);

        DUUIPipelineProfiler.documentMetaDataUpdate(name, _title, jc.size());

        final CountDownLatch jCasLock = new CountDownLatch(_pipeline.size());
        _pipeline.forEach((uuid, comp) -> 
            {
                Collection<ComponentLock> selfLocks = _locks.get(uuid).getValue0();
                Collection<ComponentLock> childLocks = _locks.get(uuid).getValue1();
                Set<String> immediateChildren = getChildren(uuid).stream()
                    .filter( child -> getHeight(child) == getHeight(uuid) + 1)
                    .collect(Collectors.toSet());

                _components.get(uuid)
                    .addTask(name, new DUUIWorker(name, comp, jc, perf, selfLocks, childLocks, immediateChildren, getHeight(uuid), jCasLock));
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


    class PipelineProgressView implements PipelineProgress {
        final Map<Integer, Set<PipelineComponent>> heightComponents;

        PipelineProgressView() {
            heightComponents = _comps.stream()
            .collect(Collectors.groupingBy(p -> p._height, Collectors.toSet()));
        }

        final AtomicLong durationPerPercentageNanos = new AtomicLong(-1l);
        long avgPerPercentageNanos = 500L;
        long prevReadTime = -1L;
        double prevReadPercentage = 0.f;
        int prevLevel = -1;

        public synchronized long acceleration() {
            final double currReadPercentage = getLevelProgress();
            final int currLevel = _currentLevel.get();

            // Reset to initial values
            if (prevLevel != currLevel) {
                prevReadPercentage = currReadPercentage;
                prevReadTime = System.nanoTime();
                // durationPerPercentageNanos.set(-1L);
                prevLevel = (currLevel);
                return durationPerPercentageNanos.get();
            }
            
            if (! (prevReadPercentage < currReadPercentage)) 
                return durationPerPercentageNanos.get();

            final long currReadTime = System.nanoTime();
            final double deltaPercentage = (currReadPercentage - prevReadPercentage) * 100.f;
            final long deltaTime = currReadTime - prevReadTime;
            final long newPercentageDuration = (long) (((double) deltaTime) / ((double) deltaPercentage));

            prevReadPercentage = currReadPercentage;
            prevReadTime = currReadTime;
            if (durationPerPercentageNanos.get() > -1L) { // Already initialized
                avgPerPercentageNanos = (long) (((double) (avgPerPercentageNanos + newPercentageDuration)) / 2.f);
                durationPerPercentageNanos.set((long) 
                    (((double) (durationPerPercentageNanos.get() + avgPerPercentageNanos)) / 2.f));

            } 
            else durationPerPercentageNanos.set(newPercentageDuration); // First initialization

            return durationPerPercentageNanos.get();
        }

        public long getAcceleration() {
            return durationPerPercentageNanos.get();
        }

        public double getLevelProgress() {
            return heightComponents.get(_currentLevel.get()).stream()
                .mapToDouble(PipelineComponent::getProgress)
                .min().orElse(0.0);
        }

        public double getComponentProgress(String uuid) {
            return _components.get(uuid).getProgress();
        }

        public int getComponentPoolSize(String uuid) {
            final int exs = _executors.size();
            final int height = _components.get(uuid)._height;
            final int divisor = exs == 1 ? heightComponents.get(height).size() : 1;
            final int psize = _components.get(uuid)._pipelineExecutor.getPoolSize();
            return (int) Math.ceil(psize / (float) divisor);
        }

        public boolean isCompleted(String uuid) {
            return _components.get(uuid).isCompleted();
        }

        public int getPipelineLevel(String uuid) {
            return _components.get(uuid)._height;
        }

        public int getCurrentLevel() {
            return _currentLevel.get();
        }

        public int getNextLevel() {
            final int n = _currentLevel.get() + 1;
            return n == _pipelineDepth + 1 ? 1 : n;
        }

        public int getLevelSize(int level) {
            return heightComponents.get(level).size();
        }

        public int getLevelSize(int level, Class<? extends IDUUIDriver> filter) {
            return (int) heightComponents.get(level).stream()
                .map(pc -> pc._uuid)
                .map(_pipeline::get)
                .map(PipelinePart::getDriver)
                .map(IDUUIDriver::getClass)
                .filter(Predicate.isEqual(filter))
                .count();
        }

        public int getLevelSize(int level, Class<? extends IDUUIDriver> ...filters) {
            return (int) heightComponents.get(level).stream()
                .map(pc -> pc._uuid)
                .map(_pipeline::get)
                .map(PipelinePart::getDriver)
                .map(IDUUIDriver::getClass)
                .filter(driver -> Stream.of(filters).anyMatch(Predicate.isEqual(driver)))
                .count();
        }

    }

    class PipelineComponent {
        final String _uuid;
        final PipelineExecutor _pipelineExecutor;
        final Map<String, DUUIWorker> _initializedWorkers = new ConcurrentHashMap<>();
        final int _height;
        final Set<String> _ancestors;
        Duration _avgExecution = Duration.ofNanos(Long.MAX_VALUE);

        int initialized = 0;
        final AtomicInteger submitted = new AtomicInteger(0);
        final AtomicInteger completed = new AtomicInteger(0);

        PipelineComponent(String uuid, PipelineExecutor executor, int height, Set<String> ancestors) {
            _uuid = uuid;
            _pipelineExecutor = executor;
            _height = height;
            _ancestors = ancestors;
        }

        void addExecTime(Duration execDuration) {
            synchronized (_avgExecution) {
                if (_avgExecution.equals(Duration.ofNanos(Long.MAX_VALUE))) {
                    _avgExecution = execDuration;
                    return;
                }
                final long sum = _avgExecution.toNanos() + execDuration.toNanos();
                final long avg = (long) (sum / 2f);
                _avgExecution = Duration.ofNanos(avg);
            }
        }

        public Duration avgDuration() {
            return _avgExecution;
        }

        long remainingDuration() {
            synchronized (_avgExecution) {
                final int poolSize = _pipelineExecutor.getPoolSize() == 0 ? 
                    1 : _pipelineExecutor.getPoolSize();
                final long avg = _avgExecution.toNanos();
                final int remaining = submitted.get() - completed.get();
                System.out.println("AVERAGE EXECUTION DURATION MS: " + _avgExecution.toMillis());
                System.out.println("REMAINING COMPONENTS: " + remaining + ", POOL SIZE: " + poolSize);
                return (long) ((remaining * avg) / poolSize);
            }
        }

        DUUIWorker getTask(String document) {
            return _initializedWorkers.remove(document);
        }

        void addTask(String document, DUUIWorker worker) {
            _initializedWorkers.put(document, worker);
            initialized++;
        }

        void submit() {
            submitted.incrementAndGet();
        }

        void complete(DUUIWorker worker) {
            addExecTime(worker._total);
            completed.incrementAndGet();
        }

        boolean canSubmit() {
            return _ancestors.stream()
                .map(_components::get)
                .allMatch(PipelineComponent::isCompleted);
        }

        boolean isSubmitted() {
            return initialized == submitted.get();
        }

        boolean isCompleted() {
            return submitted.get() == completed.get();
        }

        double getProgress() {
            return submitted.get() == 0 ? 
                0.f : ((double) completed.get()) / ((double) submitted.get());
        }

        void reset() {
            initialized = 0;
            submitted.set(0);
            completed.set(0);
        }
    }

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
            return Integer.MIN_VALUE;
            // return _currentLevel.get() == parentLevel ? 
            //     parentLevel + 1 : Integer.MIN_VALUE;
        }
    }

    class ResetterWorker implements PipelineWorker, Callable<DUUIWorker> {

        public DUUIWorker call() {
            _comps.forEach(PipelineComponent::reset);
            return null;
        }

        public int getPriority() {
            return Integer.MIN_VALUE + 1;
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

        public int getPriority() {
            return _task.getPriority();
        }

        @Override
        public String toString() {
            if (_task instanceof DUUIWorker) {
                DUUIWorker worker = (DUUIWorker) _task;
                return "component="+worker._signature+", document="+worker._name;
            }
            else return super.toString();
        }
    }

}