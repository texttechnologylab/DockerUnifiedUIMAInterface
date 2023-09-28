package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.PipelineProgress;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;

public class DUUIParallelPipelineExecutor extends DUUILinearPipelineExecutor implements IDUUIResource {

    /*
     * 
     * Taken and adapted from:
     * https://stackoverflow.com/questions/19528304/how-to-get-the-
     * threadpoolexecutor-to-increase-threads-to-max-before-queueing
     */
    class PipelineQueue extends LinkedBlockingQueue<Runnable> { // PriorityBlockingQueue
        static final long serialVersionUID = -6903933921423432194L;
        @Override
        public boolean offer(Runnable e) {
            if (_executor.getPoolSize() >= Config.strategy().getMaxPoolSize()) {
                return super.offer(e);
            }

            return false;
        }
    }

    class PipelineExecutor extends ThreadPoolExecutor {
        final BlockingQueue<DUUIWorker> _backedUp = new LinkedBlockingQueue<>();

        public PipelineExecutor(int corePoolSize, int maximumPoolSize) {
            super(corePoolSize,
                    maximumPoolSize,
                    DUUIComposer.Config.strategy().getTimeout(TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS,
                    new PipelineQueue());

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
        protected void beforeExecute(Thread t, Runnable r) {
            if (r instanceof ComparableFutureTask) {
                final ComparableFutureTask<?> c = (ComparableFutureTask<?>) r;
                if (c.getPriority() > 0)
                    _currentLevel.set(c.getPriority());
            }
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            long start = System.nanoTime();
            if (!(r instanceof Future<?>))
                return;

            Object result = null;
            try {
                result = ((Future<?>) r).get();
                if (result == null || !(result instanceof DUUIWorker))
                    return;

                final DUUIWorker worker = (DUUIWorker) result;
                _currentLevel.set(worker.getPriority());
                final PipelineComponent cp = _components.get(worker.component());
                // final String children = worker._childrenIds.stream()
                //     .map(_pipeline::get)
                //     .map(PipelinePart::getSignature)
                //     .map(Signature::toString)
                //     .collect(Collectors.joining("; "));

                cp.complete(worker);
                _view.acceleration();

                // System.out.println("--------------------------------------------\n"
                //     + String.format("%s: INIT %d SUBMIT %d COMPLETE %d \n", 
                //         worker._signature, cp.initialized, cp.submitted.get(), cp.completed.get())
                //     + String.format("BACKED-UP %d FAILED %d \n", _backedUp.size(), _failedWorkers.size())
                //     + "CHILDREN: " + children + "\n"
                //     + "--------------------------------------------\n");

                tryFinalizeCas(worker);
                worker._childrenIds.forEach(child -> {
                    final PipelineComponent component = _components.get(child);
                    submit(worker._name, component);
                });
            } catch (ExecutionException ee) {
                t = ee.getCause();
                if (t instanceof AnnotatorUnreachableException) {
                    t.printStackTrace();
                    AnnotatorUnreachableException e = (AnnotatorUnreachableException) t;
                    final String workerId = e.failedWorker._name + e.failedWorker.component();
                    if (_failedWorkers.contains(workerId) || !e.resuable) {
                        try {
                            _failedWorkers.remove(workerId);
                            _components.get(e.failedWorker.component()).complete(e.failedWorker);
                            tryFinalizeCas(e.failedWorker);
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                            e1.printStackTrace();
                        }
                    } else { // Reschedule
                        submit(e.failedWorker);
                        _failedWorkers.add(workerId);
                    }
                }
            } catch (InterruptedException | CancellationException ie) {
                Thread.currentThread().interrupt();
            } catch (RejectedExecutionException e) {
                System.out.printf("[DUUIParallelPipeline] Worker rejected: %s %n", result);
                e.printStackTrace();
            } finally {
                DUUIComposer.totalafterworkerwait.getAndAdd(System.nanoTime() - start);
            }
        }

        void tryFinalizeCas(DUUIWorker worker) throws InterruptedException {
            worker._jCasLock.countDown();
            final long count = worker._jCasLock.getCount();
            if (worker._jCasLock.await(1, TimeUnit.NANOSECONDS) && count == 0L) {
                JCasCompletionWorker w = new JCasCompletionWorker(worker._jc, worker.getPriority(), worker._perf);
                if (_levelSynchronized) {
                    _backedUpJcas.add(w);
                    if (isSubmitted.getAsBoolean()) {
                        new ResetterWorker().call();
                        while ((w = _backedUpJcas.poll()) != null) {
                            submit(w);
                        }
                    } 
                } else submit(w);
            }
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            if (task instanceof DUUIWorker) {
                DUUIWorker worker = (DUUIWorker) task;
                final String workerId = worker._name + worker.component();
                final PipelineComponent component = _components.get(worker.component());
                if (!_failedWorkers.contains(workerId))
                    component.submit();
            }
            return super.submit(task);
        }

        public void submit(String document, PipelineComponent component) {
            DUUIWorker worker = component.getTask(document);
            if (worker == null) return;
            if (_levelSynchronized) {
                if (component.canSubmit()) {
                    submit(worker);
                    while ((worker = _backedUp.poll()) != null) {
                        submit(worker);
                    }
                } else _backedUp.add(worker);
            } else submit(worker);
        }
    }

    final Map<String, PipelineComponent> _components;
    final PipelineExecutor _executor;
    final List<PipelineComponent> _comps;

    final BooleanSupplier isComplete;
    final BooleanSupplier isSubmitted;

    final AtomicInteger _registeredDocuments = new AtomicInteger(0);
    final BlockingQueue<JCasCompletionWorker> _backedUpJcas = new LinkedBlockingQueue<>();
    final HashSet<String> _failedWorkers = new HashSet<>();
    boolean _levelSynchronized = false;
    final AtomicLong _remainingDuration = new AtomicLong(Long.MAX_VALUE);

    final AtomicInteger _currentLevel = new AtomicInteger(1);
    final Set<String> _topNodes = new HashSet<>();
    final List<PipelineComponent> _lastLevel;
    final int _pipelineDepth;

    final PipelineProgressView _view;

    public DUUIParallelPipelineExecutor(Vector<PipelinePart> flow) {
        this(flow, Integer.MAX_VALUE);
    }

    public DUUIParallelPipelineExecutor(Vector<PipelinePart> flow, int maxWidth) {
        super(flow, maxWidth);

        ResourceManager.register(this);

        _components = new ConcurrentHashMap<>((int) (_pipeline.size() * 1.25));

        final int corePoolSize = Config.strategy().getCorePoolSize();
        final int maxPoolSize = Config.strategy().getMaxPoolSize();

        _executor = new PipelineExecutor(corePoolSize, maxPoolSize);

        for (String node : _executionplan) {
            _components.put(node, 
                new PipelineComponent(
                    node, 
                    _pipeline.get(node).getDriver(), 
                    _pipeline.get(node).getSignature(),
                    getHeight(node), 
                    getAncestors(node).stream()
                    .filter(p -> getHeight(node) - 1 == getHeight(p))
                    .collect(Collectors.toSet())
                )
            );

            if (getAncestors(node).size() == 0)
                _topNodes.add(node);
        }
        
        _comps = _components.values().stream().collect(Collectors.toUnmodifiableList());

        _pipelineDepth = _comps.stream()
                .map(x -> x._height)
                .max(Integer::compare)
                .orElse(1);
        _lastLevel = _comps.stream()
            .filter(x -> x._height == _pipelineDepth)
            .collect(Collectors.toUnmodifiableList());
        isComplete = () -> {
            for (PipelineComponent comp : _lastLevel) {
                if (!comp.isCompleted()) return false;
            }
            return true;
        };
        
        isSubmitted = () -> {
            for (PipelineComponent comp : _lastLevel) {
                if (!comp.isSubmitted()) return false;
            }
            return true;
        };

        _view = new PipelineProgressView();
    }

    public DUUIParallelPipelineExecutor withLevelSynchronization(boolean synchronize) {
        _levelSynchronized = synchronize;
        return this;
    }

    @Override
    public void scale(ResourceViews statistics) {
        if (!_levelSynchronized) return; 
        _comps.forEach(component -> { // TODO: Speed
            if (_executor._backedUp.size() > 0) {
                if (_view.getNextLevel() != component._height) return;
                if (!component.canSubmit()) return;
                DUUIWorker w = null;
                while ((w = _executor._backedUp.poll()) != null) {
                    _executor.submit(w);
                }
            }
        });
        if (isSubmitted.getAsBoolean()) {
            JCasCompletionWorker j = null;
            while ((j = _backedUpJcas.poll()) != null) {
                _executor.submit(j);
            }
        }
    }

    @Override
    public PipelineProgress collect() {
        return _view;
    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        final long start = System.nanoTime();
        _registeredDocuments.incrementAndGet();
        _comps.stream() // TODO: Speed
        .filter(p -> p._ancestors.size() == 0)
        .forEach(component -> {
            final DUUIWorker runner = getTask(name, component._uuid, jc, perf);
            _executor.submit(runner);
        });
        DUUIComposer.totalafterworkerwait.getAndAdd(System.nanoTime() - start);
    }

    public void shutdown() throws Exception {
        if (_levelSynchronized)
            getResourceManager().setBatchReadIn(true);

        while (!isComplete.getAsBoolean() 
            || _backedUpJcas.size() > 0 
            || _executor.getQueue().size() > 0) {
            Thread.sleep(500);
        }
        _executor.shutdown();
        while (!_executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {}
        destroy();
    }

    public void destroy() {
        _executor.shutdownNow();
    }


    public DUUIWorker getTask(String name, String uuid, JCas jc, DUUIPipelineDocumentPerformance perf) {
        DUUIWorker w = null;
        if ((w = _components.get(uuid).getTask(name)) != null)
            return w;

        initialiseTasks(name, jc, perf);

        return _components.get(uuid).getTask(name);
    }

    public void initialiseTasks(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        // TODO: create view of jcas to allow for non-blocking thread-safe read.
        
        // typeCheck(jc);

        final CountDownLatch jCasLock = new CountDownLatch(_pipeline.size());
        _comps.forEach(comp -> {
            Set<String> immediateChildren = getChildren(comp._uuid).stream()
                    .filter(child -> getHeight(child) == getHeight(comp._uuid) + 1)
                    .collect(Collectors.toSet());

            comp.addTask(name, 
                new DUUIWorker(name, 
                    _pipeline.get(comp._uuid), 
                    jc, 
                    perf, 
                    immediateChildren, 
                    getHeight(comp._uuid), 
                    jCasLock)
            );
        });
    }

    class PipelineProgressView implements PipelineProgress {
        final Map<Integer, Set<PipelineComponent>> heightComponents;

        PipelineProgressView() {
            heightComponents = _comps.stream()
                    .collect(Collectors.groupingBy(p -> p._height, Collectors.toUnmodifiableSet()));
        }

        final ReentrantLock lock = new ReentrantLock(true);
        final AtomicLong nanosPerPercentage = new AtomicLong(100L * 1_000_000L); // 100ms as reference value
        long avgNanosPerPercentage = 100L * 1_000_000;
        double prevReadPercentage = 0.f;
        long prevReadTime = System.nanoTime();

        void reset() {
            lock.lock();
            try {
                final double currReadPercentage = getLevelProgress();
                prevReadPercentage = prevReadPercentage > currReadPercentage ? 0.f : currReadPercentage;
                prevReadTime = System.nanoTime();
            } finally {
                lock.unlock();
            }
        }

        long acceleration() {
            lock.lock();
            try {
                final double currReadPercentage = getLevelProgress();
                final long currReadTime = System.nanoTime();
                final double deltaPercentage = (currReadPercentage - prevReadPercentage) * 100.f;
                final long deltaTime = currReadTime - prevReadTime;
                final long newPercentageDuration = Long
                        .max(100L * 1_000_000, (long) (deltaTime / (double) deltaPercentage));

                final long nanos = nanosPerPercentage.get();
                avgNanosPerPercentage = (long) ((avgNanosPerPercentage + newPercentageDuration) / 2.f);
                final long newNanos = Long
                        .max(100L * 1_000_000, (long) ((nanos + avgNanosPerPercentage) / 2.f));

                prevReadPercentage = currReadPercentage;
                prevReadTime = currReadTime;

                nanosPerPercentage.set(newNanos);
                return newNanos;
            } finally {
                lock.unlock();
            }
        }

        public long getAcceleration() {
            return nanosPerPercentage.get();
        }

        public double getLevelProgress() {
            return heightComponents.get(_currentLevel.get()).stream()
                    .mapToDouble(PipelineComponent::getProgress)
                    .average().orElse(0.0);
        }

        public double getComponentProgress(String uuid) {
            return _components.get(uuid).getProgress();
        }

        public int getComponentPoolSize(String uuid) {
            final int height = _components.get(uuid)._height;
            final int psize = _executor.getPoolSize();
            return (int) Math.ceil(psize / (float) heightComponents.get(height).size());
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

        public int getLevelSize(int level, Class<? extends IDUUIDriver> filter) { // TODO: Speed
            return (int) heightComponents.get(level).stream()
                    .map(pc -> pc._uuid)
                    .map(_pipeline::get)
                    .map(PipelinePart::getDriver)
                    .map(IDUUIDriver::getClass)
                    .filter(Predicate.isEqual(filter))
                    .count();
        }

        public int getLevelSize(int level, Class<? extends IDUUIDriver>... filters) { // TODO: Speed
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
        final IDUUIDriver _driver;
        final Signature _sig;
        final Map<String, DUUIWorker> _initializedWorkers = new ConcurrentHashMap<>();
        final int _height;
        final List<PipelineComponent> _ancestors;

        final AtomicInteger initialized = new AtomicInteger(0);
        final AtomicInteger submitted = new AtomicInteger(0);
        final AtomicInteger completed = new AtomicInteger(0);

        PipelineComponent(String uuid, IDUUIDriver driver, Signature sig, int height, Set<String> ancestors) {
            _uuid = uuid;
            _driver = driver; 
            _sig = sig;
            _height = height;
            _ancestors = ancestors.stream()
                .map(_components::get)
                .collect(Collectors.toUnmodifiableList());
        }

        DUUIWorker getTask(String document) {
            return _initializedWorkers.remove(document);
        }

        void addTask(String document, DUUIWorker worker) {
            _initializedWorkers.put(document, worker);
            initialized.getAndIncrement();
        }

        void submit() {
            submitted.incrementAndGet();
        }

        void complete(DUUIWorker worker) {
            completed.incrementAndGet();
        }

        boolean canSubmit() {
            if (!getResourceManager().isBatchReadIn()) return false;
            for (PipelineComponent parent : _ancestors) {
                if (!parent.isSubmitted()) return false;
            }
            return true;
        }

        boolean isInitialized() {
            final int i = initialized.get();
            final int borrowed = getResourceManager().getBorrowedCASCount();
            return i == borrowed;
        }

        boolean isSubmitted() {
            final int i = initialized.get();
            final int s = submitted.get();
            final int borrowed = getResourceManager().getBorrowedCASCount();
            System.out.printf("COMPONENT %s IS SUBMITTED INIT %d SUBMIT %d BORROWED %d \n", 
                _sig, i, s, borrowed);
            return i == borrowed && i == s;
        }

        boolean isCompleted() {
            return _initializedWorkers.isEmpty() 
                && initialized.get() == submitted.get() 
                && submitted.get() == completed.get();
        }

        double getProgress() {
            return submitted.get() == 0 ? 
                0.f : ((double) completed.get()) / ((double) submitted.get());
        }

        void reset() {
            initialized.set(0);
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
        final DUUIPipelineDocumentPerformance perf;

        public JCasCompletionWorker(JCas jc, int parentLevel, DUUIPipelineDocumentPerformance _perf) {
            this.jc = jc;
            this.parentLevel = parentLevel;
            this.perf = _perf;
        }

        public DUUIWorker call() {
            if (DUUIComposer.Config.storage() != null)
                DUUIComposer.Config.storage().addMetricsForDocument(perf);
            DUUIComposer.Config.write(jc);
            ResourceManager.getInstance().returnCas(this.jc);
            return null;
        }

        public int getPriority() {
            return Integer.MIN_VALUE;
            // return _currentLevel.get() == parentLevel ?
            // parentLevel + 1 : Integer.MIN_VALUE;
        }
    }

    class ResetterWorker implements PipelineWorker, Callable<DUUIWorker> {

        public DUUIWorker call() {
            _view.reset();
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
    class ComparableFutureTask<T> extends FutureTask<T> implements Comparable<ComparableFutureTask<T>> {

        final PipelineWorker _task;
        final long _timestamp;

        public ComparableFutureTask(Callable<T> task) {
            super(task);
            _task = (PipelineWorker) task;
            _timestamp = System.nanoTime();
        }

        @Override
        public int compareTo(ComparableFutureTask<T> that) {
            if (getPriority() == that.getPriority())
                return Long.compare(_timestamp, that._timestamp);
            return Integer.compare(getPriority(), that.getPriority());
        }

        public int getPriority() {
            return _task.getPriority();
        }

        @Override
        public String toString() {
            if (_task instanceof DUUIWorker) {
                DUUIWorker worker = (DUUIWorker) _task;
                return "component=" + worker._signature + ", document=" + worker._name;
            } else
                return super.toString();
        }
    }

}