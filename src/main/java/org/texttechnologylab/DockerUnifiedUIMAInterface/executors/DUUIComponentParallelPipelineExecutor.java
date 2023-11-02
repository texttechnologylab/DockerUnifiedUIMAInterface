package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.uima.cas.CASException;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Marker;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCopier;
import org.apache.uima.util.CasCreationUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.PipelineProgress;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token_Type;

public class DUUIComponentParallelPipelineExecutor extends DUUILinearPipelineExecutor implements IDUUIResource {

    class PipelineExecutor extends ThreadPoolExecutor {
        final BlockingQueue<DUUITask> _backedUp = new LinkedBlockingQueue<>();
        final BlockingQueue<JCasCompletionWorker> _backedUpJcas = new LinkedBlockingQueue<>();
        final Set<String> _failedWorkers = ConcurrentHashMap.newKeySet();

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
        }

        void afterWorker(DUUITask worker) throws InterruptedException {
            final PipelineComponent cp = _components.get(worker.component());
            _currentLevel.set(worker.getPriority());
            cp.complete(worker);
            _view.acceleration();
            // final String children = worker._childrenIds.stream()
            //     .map(_pipeline::get)
            //     .map(PipelinePart::getSignature)
            //     .map(Signature::toString)
            //     .collect(Collectors.joining("; "));

            // System.out.println("--------------------------------------------\n"
            //     + String.format("COMPONENT %s PROGRESS %.2f \n", cp._sig, cp.getProgress()*100.f) 
            //     + String.format("%s: INIT %d SUBMIT %d COMPLETE %d \n", 
            //         worker._signature, cp.initialized.get(), cp.submitted.get(), cp.completed.get())
            //     + String.format("BACKED-UP %d FAILED %d \n", _backedUp.size(), _failedWorkers.size())
            //     + "CHILDREN: " + children + "\n"
            //     + "--------------------------------------------\n");
            tryFinalizeCas(worker);
            worker._childrenIds.forEach(child -> {
                final PipelineComponent component = _components.get(child);
                submit(worker._name, component);
            });
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            long start = System.nanoTime();
            if (!(r instanceof Future<?>))
                return;

            Object result = null;
            try {
                result = ((Future<?>) r).get();
                if (result == null || !(result instanceof DUUITask))
                    return;

                final DUUITask worker = (DUUITask) result;
                afterWorker(worker);
                
            } catch (ExecutionException ee) {
                t = ee.getCause();
                if (!(t instanceof AnnotatorUnreachableException)) {
                    t.printStackTrace();
                    
                    return;
                }
                AnnotatorUnreachableException e = (AnnotatorUnreachableException) t;
                final String workerId = e.failedWorker._name + e.failedWorker.component();

                if (_reschedule) {
                    if (!_failedWorkers.contains(workerId) && e.resuable) { // Reschedule
                        _failedWorkers.add(workerId);
                        submit(e.failedWorker);
                        return;
                    } 
                }

                try {
                    _failedWorkers.remove(workerId);
                    afterWorker(e.failedWorker);
                } catch (InterruptedException e1) {
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

        void tryFinalizeCas(DUUITask worker) throws InterruptedException {
            worker._jCasLock.countDown();
            final long count = worker._jCasLock.getCount();
            if (worker._jCasLock.await(1, TimeUnit.NANOSECONDS) && count == 0L) {
                JCasCompletionWorker w = new JCasCompletionWorker(worker._jc, worker.getPriority(), worker._perf);
                if (_levelSynchronized && !_view.hasShutdown()) {
                    _backedUpJcas.add(w);
                    if (isCompleted.getAsBoolean()) {
                        _view.reset();
                        _comps.forEach(PipelineComponent::reset);
                        while ((w = _backedUpJcas.poll()) != null) {
                            submit(w);
                        }
                    } 
                } else submit(w);
            }
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            if (task instanceof DUUITask) {
                DUUITask worker = (DUUITask) task;
                final String workerId = worker._name + worker.component();
                final PipelineComponent component = _components.get(worker.component());
                if (!_failedWorkers.contains(workerId))
                    component.submit();
            }
            return super.submit(task);
        }

        public void submit(String document, PipelineComponent component) {
            DUUITask worker = component.getTask(document);
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

    final BooleanSupplier isCompleted;
    final BooleanSupplier isSubmitted;

    boolean _levelSynchronized = false;
    boolean _reschedule = false;
    boolean _shutdown = false;

    final AtomicInteger _currentLevel = new AtomicInteger(1);
    final List<PipelineComponent> _lastLevel;
    final int _pipelineDepth;
    final int _pipelineWidth; 

    final PipelineProgressView _view;

    public DUUIComponentParallelPipelineExecutor(Vector<PipelinePart> flow) {
        this(flow, Integer.MAX_VALUE);
    }

    public DUUIComponentParallelPipelineExecutor(Vector<PipelinePart> flow, int maxWidth) {
        super(flow, maxWidth);

        ResourceManager.register(this);

        _components = new ConcurrentHashMap<>((int) (_pipeline.size() * 1.25));

        final int corePoolSize = Config.strategy().getMaxPoolSize();
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
        }
        
        _comps = _components.values().stream().collect(Collectors.toUnmodifiableList());

        _pipelineDepth = _comps.stream()
                .map(x -> x._height)
                .max(Integer::compare)
                .orElse(1);
        _lastLevel = _comps.stream()
            .filter(x -> x._height == _pipelineDepth)
            .collect(Collectors.toUnmodifiableList());
        isCompleted = () -> {
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
        _pipelineWidth = _view.heightComponents.values().stream().mapToInt(Set::size).max().orElse(1);
    }

    public DUUIComponentParallelPipelineExecutor withLevelSynchronization(boolean synchronize) {
        _levelSynchronized = synchronize;
        return this;
    }

    public DUUIComponentParallelPipelineExecutor withFailedWorkerRescheduling(boolean reschedule) {
        _reschedule = reschedule;
        return this;
    }


    // @Override
    // public void scale(ResourceViews statistics) { 
    //     // TODO: Dynamic Scaling of threads by through PipelineExecutor.setMaximumPoolSize()
    // }

    @Override
    public PipelineProgress collect() {
        return _view;
    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        final long start = System.nanoTime();
        _view.totalDocuments.incrementAndGet();
        _view.liveDocuments.incrementAndGet();

        _comps.stream()
        .filter(p -> p._ancestors.size() == 0)
        .forEach(component -> {
            final DUUITask runner = getTask(name, component._uuid, jc, perf);
            _executor.submit(runner);
        });
        DUUIComposer.totalafterworkerwait.getAndAdd(System.nanoTime() - start);
    }

    public void shutdown() throws Exception {
        _shutdown = true;
        if (_levelSynchronized)
            getResourceManager().setBatchReadIn(true);

        while (!isCompleted.getAsBoolean() 
            || _executor._backedUpJcas.size() > 0 
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


    public DUUITask getTask(String name, String uuid, JCas jc, DUUIPipelineDocumentPerformance perf) {
        DUUITask w = null;
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
                new DUUITask(name, 
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
        final AtomicInteger totalDocuments = new AtomicInteger(0);
        final AtomicInteger liveDocuments = new AtomicInteger(0);

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

        public long getRemainingNanos() {
            final long accel = nanosPerPercentage.get();
            final double progress = getLevelProgress();
            final double remainingPercentage = 100.f - progress * 100.f;
            final long remainingNanos = (long) remainingPercentage * accel; 
            final double queueratio = (_executor.getQueue().size() * 1.1)  / (double) _executor.getPoolSize(); 
            final long correction = (long) queueratio * accel;
            return remainingNanos - correction;
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

        public int getPipelineDepth() {
            return _pipelineDepth;
        }

        public int getCurrentLevel() {
            return _currentLevel.get();
        }

        public int getNextLevel() {
            final int n = _currentLevel.get() + 1;
            return n == _pipelineDepth + 1 ? 1 : n;
        }

        public boolean hasShutdown() {
            return _shutdown;
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
        final Map<String, DUUITask> _initializedWorkers = new ConcurrentHashMap<>();
        final int _height;
        final List<PipelineComponent> _ancestors;

        final AtomicInteger initialized = new AtomicInteger(0);
        final AtomicInteger submitted = new AtomicInteger(0);
        final AtomicInteger alive = new AtomicInteger(0);
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

        DUUITask getTask(String document) {
            return _initializedWorkers.remove(document);
        }

        void addTask(String document, DUUITask task) {
            _initializedWorkers.put(document, task);
            initialized.getAndIncrement();
        }

        void submit() {
            submitted.incrementAndGet();
            alive.incrementAndGet();
        }

        void complete(DUUITask task) {
            completed.incrementAndGet();
            alive.decrementAndGet();
        }

        boolean canSubmit() {
            if (!getResourceManager().isBatchReadIn()) return false;
            for (PipelineComponent parent : _ancestors) {
                if (!parent.isInitialized() || !parent.isSubmitted()) return false;
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
            // final int borrowed = getResourceManager().getBorrowedCASCount();
            // System.out.printf("COMPONENT %s IS SUBMITTED INIT %d SUBMIT %d BORROWED %d \n", 
            //     _sig, i, s, borrowed);
            return i == s && _initializedWorkers.size() == 0;
        }

        boolean isCompleted() {
            return _initializedWorkers.isEmpty() 
                && initialized.get() == submitted.get() 
                && submitted.get() == completed.get();
        }

        double getProgress() {
            final int required = submitted.get() - _executor.getPoolSize();
            return required <= 0 ? 
                0.f : Math.min(completed.get() / (double) required, 1.f);
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

    class JCasCompletionWorker implements PipelineWorker, Callable<DUUITask> {

        final JCas jc;
        final int parentLevel;
        final DUUIPipelineDocumentPerformance perf;

        public JCasCompletionWorker(JCas jc, int parentLevel, DUUIPipelineDocumentPerformance _perf) {
            this.jc = jc;
            this.parentLevel = parentLevel;
            this.perf = _perf;
        }

        public DUUITask call() {
            _view.liveDocuments.decrementAndGet();
            if (DUUIComposer.Config.storage() != null)
                DUUIComposer.Config.storage().addMetricsForDocument(perf);
            ResourceManager.getInstance().returnCas(this.jc);
            return null;
        }

        public int getPriority() {
            return Integer.MIN_VALUE;
            // return _currentLevel.get() == parentLevel ?
            // parentLevel + 1 : Integer.MIN_VALUE;
        }
    }

    class PipelineQueue extends LinkedBlockingQueue<Runnable> { // PriorityBlockingQueue
        static final long serialVersionUID = -6903933921423432194L;
        final LinkedBlockingDeque<ComparableFutureTask<?>> backpressure = new LinkedBlockingDeque<>();
        final LinkedBlockingDeque<ComparableFutureTask<?>> backpressure2 = new LinkedBlockingDeque<>();
        // final AtomicBoolean switch_ = new AtomicBoolean(false);
        boolean switch_ = false;

        @Override
        public boolean offer(Runnable e) {
            final ComparableFutureTask<?> c = (ComparableFutureTask<?>) e;
            
            if (_executor.getPoolSize() >= Config.strategy().getMaxPoolSize()) {
                return super.offer(e);
                // if (_pipelineWidth == 1) return super.offer(e);
                
                // if (!_levelSynchronized) {
                //     ComparableFutureTask<?> b;
                //     if (_view.hasShutdown() && isSubmitted.getAsBoolean()) {
                //         while ((b = (switch_ = !switch_) ? backpressure2.pollLast() : backpressure2.pollFirst()) != null) {
                //             super.offer(b);
                //         }
                //         return super.offer(c);
                //     }

                //     b = (switch_ = !switch_) ? backpressure2.pollLast() : backpressure2.pollFirst();
                //     super.offer(b);
                //     return (switch_ = !switch_) ? backpressure2.offerFirst(c) : backpressure2.offerLast(c);
                // } 

                // Task mangling to reduce probability of sibling tasks being scheduled together 

                // if (!(c._task instanceof DUUIWorker)) return super.offer(e);
                // final boolean wide = _view.heightComponents.getOrDefault(c.getPriority(), new HashSet<>(0)).size() > 1;
                // if (!wide) return super.offer(e);



                // final PipelineComponent pc = _components.get(((DUUIWorker) c._task).component());
                // ComparableFutureTask<?> b = (switch_ = !switch_) ? backpressure.pollLast() : backpressure.pollFirst();
                // if (b == null) {
                //     if (!pc.isSubmitted()) 
                //         return (switch_ = !switch_) ? backpressure.offerFirst(c) : backpressure.offerLast(c);
                //     else return super.offer(c);
                // }
                // else {
                //     final boolean isChild = b.getPriority() < c.getPriority();
                //     if (pc.isSubmitted() || isChild) {
                //         super.offer(b);
                //         if (!isChild) super.offer(c);
                //         while ((b = (switch_ = !switch_) ? backpressure.pollLast() : backpressure.pollFirst()) != null) {
                //             super.offer(b);
                //         }
                //         if (isChild) backpressure.offerLast(c);
                //         return true;
                //     }
                //     if (b.isSibling(c) || backpressure.size() < _executor.getMaximumPoolSize()) 
                //         return ((switch_ = !switch_) ? 
                //             backpressure.offerFirst(b) && backpressure.offerFirst(c) : backpressure.offerLast(b) && backpressure.offerLast(c));
                //     else if (b.isSameComp(c)) 
                //         return super.offer(b) && super.offer(c);
                //     else return super.offer(b) && super.offer(c);
                // }
            }
            return false;
        }
    }
    
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

        public boolean isSibling(ComparableFutureTask<?> that) {
            final boolean bothWorkers = 
                _task instanceof DUUITask && that._task instanceof DUUITask;
            if (bothWorkers) {
                final DUUITask w1 = (DUUITask) _task;
                final DUUITask w2 = (DUUITask) that._task;
                return w1._jc == w2._jc && w1._height == w2._height;
            } else return false;
        }

        public boolean isSameComp(ComparableFutureTask<?> that) {
            final boolean bothWorkers = 
                _task instanceof DUUITask && that._task instanceof DUUITask;
            if (bothWorkers) {
                final DUUITask w1 = (DUUITask) _task;
                final DUUITask w2 = (DUUITask) that._task;
                return w1._component == w2._component;
            } else return false;
        }

        public int getPriority() {
            return _task.getPriority();
        }

        @Override
        public String toString() {
            if (_task instanceof DUUITask) {
                DUUITask worker = (DUUITask) _task;
                return "component=" + worker._signature + ", document=" + worker._name;
            } else
                return super.toString();
        }
    }

    
    class DUUITask implements Callable<DUUITask>, PipelineWorker {
    public final String _name;
    public final PipelinePart _component;
    public final JCas _jc;
    public final DUUIPipelineDocumentPerformance _perf;
    public final Signature _signature;
    public final String _threadName; 
    public final Set<String> _childrenIds;
    public final int _height;
    public final CountDownLatch _jCasLock;
    Duration _total = Duration.ofSeconds(0);
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024);

    public DUUITask(String name, 
                PipelinePart component, 
                JCas jc, 
                DUUIPipelineDocumentPerformance perf,
                Set<String> children,
                int height, 
                CountDownLatch jCasLock) {
        _name = name;
        _component = component; 
        _jc = jc; 
        _perf = perf;
        _signature = _component.getSignature(); 
        _childrenIds = children;
        _height = height;
        _jCasLock = jCasLock;

        _threadName = String.format("%s-%s", _name, _signature);
    }

    @Override
    public DUUITask call() throws Exception {
        long start = System.nanoTime();
        final boolean hasSiblings = _view.heightComponents.get(_height).size() > 1;
        JCas uCas = _jc;
        Marker m = null;
        
        Thread.currentThread().setName(_threadName);
        ResourceManager.register(Thread.currentThread(), true);
        if (!typeCheck(_jc)) {
            System.out.printf("[%s] failed type checking.%n", _threadName + "-" + _component.getSignature());
            return this;
        }
        try {
            // if (hasSiblings) {
                
            //     uCas = getResourceManager().takeInterCas();
            //     // uCas = CasCreationUtils
            //     //     .createCas(_jc.getTypeSystem(), null, null, null)
            //     //     .getJCas();
            //     System.out.printf("[%s] take cas %s.%n",_threadName, uCas);
            //     if (uCas == null) {
            //         System.out.println("FUZUUUUCK");
            //     } else {
            //         synchronized(_jc) {
            //             Serialization.serializeCAS(_jc.getCas(), out);
            //         }
            //         Serialization.deserializeCAS(uCas.getCas(), new ByteArrayInputStream(out.toByteArray()));
            //         m = uCas.getCas().createMarker();
            //         if (!m.isValid()) {
            //             System.out.println("FUUUUCK");
            //         }
            //         DUUIComposer.totalafterworkerwait.getAndAdd(System.nanoTime() - start);
                        
            //     }
            // }
            
            System.out.printf("[%s] starting analysis %s.%n", _threadName, uCas);
            _component.run(_name, _jc, _perf); 
            System.out.printf("[%s] finished analysis.%n", _threadName);
            
            
            // if (hasSiblings && uCas != null ) {
            //     start = System.nanoTime();
                
            //     int b = 0;
            //     int e = 0;
            //     boolean s = false;
            //     synchronized(_jc) {
            //         Serialization.deserializeCAS(_jc.getCas(), new ByteArrayInputStream(out.toByteArray()));

            //         CasCopier c2 = new CasCopier(uCas.getCas(), _jc.getCas());
            //         final Collection<? extends Annotation> jss = JCasUtil.select(uCas, Annotation.class);
            //         for (Class<? extends Annotation> output : _signature.getOutputs()) {
            //             final Collection<? extends Annotation> outs = JCasUtil.select(uCas, output);
            //             System.out.println(_threadName + " : " + output.getSimpleName() + " " + outs.size());
            //             for (Annotation annotation : outs) {
            //                 if (m.isNew(annotation)) {
            //                     Annotation tgt = (Annotation) c2.copyFs(annotation);
            //                     tgt.addToIndexes();
            //                 }
            //             }
            //         }

            //         // for (Class<? extends Annotation> input : _signature.getInputs()) {
            //         //     final Collection<? extends Annotation> outs = JCasUtil.select(uCas, input);
            //         //     System.out.println(_threadName + " : " + input.getSimpleName() + " " + outs.size());
            //         //     for (Annotation annotation : outs) {
            //         //         if (m.isModified(annotation)) {
            //         //             Annotation anSrc = annotation;
            //         //             Annotation tgt = JCasUtil.selectAt(_jc, input, anSrc.getBegin(), anSrc.getEnd()).get(0);
            //         //             for (Feature feat : tgt.getType().getFeatures()) {
            //         //                 for (Feature feat2 : anSrc.getType().getFeatures()) {
            //         //                     final boolean prim = feat.getRange().isPrimitive();
            //         //                     if (prim) {
            //         //                         String f2 = anSrc.getFeatureValueAsString(feat2);
            //         //                         final boolean isnull = tgt.getFeatureValueAsString(feat) == null;
            //         //                         if (isnull) {
            //         //                             tgt.setFeatureValueFromString(feat, f2);
            //         //                         } else if (tgt.getFeatureValueAsString(feat).equals(f2)) {
            //         //                             tgt.setFeatureValueFromString(feat, f2);
            //         //                         }
            //         //                         continue;
            //         //                     }

            //         //                     if (feat.getName().equals(feat2.getName())) {
            //         //                         if (anSrc.getFeatureValue(feat2) == null) continue;
            //         //                         // System.out.println("FEATURE " + feat2);
            //         //                         // System.out.println("FEATURE STRUCTURE " + anSrc);
            //         //                         // System.out.println("FEATURE STRUCTURE " + tgt);
            //         //                         final FeatureStructure fs = anSrc.getFeatureValue(feat2);
            //         //                         final boolean isnull = tgt.getFeatureValue(feat) == null;
            //         //                         if (isnull) {
                                                
            //         //                             final Annotation fs2 = (Annotation) fs;
            //         //                             Annotation tgtn = JCasUtil.selectAt(_jc, fs2.getClass(), fs2.getBegin(), fs2.getEnd()).get(0);
            //         //                             tgt.setFeatureValue(feat, tgtn);
            //         //                         } else if (!tgt.getFeatureValue(feat).equals(fs)) {
            //         //                             if (!(fs instanceof Annotation)) continue;
            //         //                             final Annotation fs2 = (Annotation) fs;
            //         //                             Annotation tgtn = JCasUtil.selectAt(_jc, fs2.getClass(), fs2.getBegin(), fs2.getEnd()).get(0);
            //         //                             tgt.setFeatureValue(feat, tgtn);
                                                
            //         //                         }
            //         //                     }
            //         //                 }
            //         //             }
            //         //         }
            //         //     }
            //         // }
                    
            //         // Token t1 = JCasUtil.select(_jc, Token.class).stream().findFirst().orElse(null);
            //         // if (t1 != null) {
            //         //     System.out.printf("[%s] Token\n", _threadName);
            //         //     System.out.println(t1);
            //         // }
            //     }
            //     // out.reset();
            //     // synchronized(_jc) {
            //     // }
            //     getResourceManager().returnInterCas(uCas);
            //     DUUIComposer.totalafterworkerwait.getAndAdd(System.nanoTime() - start);
            // }
  
        } catch (AnnotatorUnreachableException e) { 
            e.setFailedWorker(this); // Task might be rescheduled
            throw e;
        } catch (Exception e) { // Task won't be rescheduled
        
            // if (uCas != _jc) getResourceManager().returnInterCas(uCas);
            throw new AnnotatorUnreachableException(this, 
                new RuntimeException(String.format("[%s] Pipeline component failed.%n", _threadName), e));
        }

        return this; 
    }

    public Duration total() {
        return _total;
    }

    public int getPriority() {
        return _height;
    }

    public String component() {
        return _component.getUUID();
    }


    public String toString() {
        return "Component="+_signature+", Document"+_name;
    }
}

}