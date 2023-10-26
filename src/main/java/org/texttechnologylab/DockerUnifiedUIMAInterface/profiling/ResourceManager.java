package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling;

import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatb;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.DockerDriverView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.executors.strategy.PoolStrategy;

public class ResourceManager {

    final AtomicBoolean _finished = new AtomicBoolean(false);
    final CountDownLatch finishLock = new CountDownLatch(1);

    CasPool _casPool;     

    SystemResourceViews _system; 
    boolean memoryCritical = false;
    final ReentrantLock memoryLock = new ReentrantLock();
    final Condition unpaused = memoryLock.newCondition();
    final AtomicBoolean _batchRead = new AtomicBoolean(false);
    final ScheduledThreadPoolExecutor _executor = new ScheduledThreadPoolExecutor(1);


    static ResourceManager _rm = new ResourceManager();
    
    public ResourceManager() {
        _system = new SystemResourceViews();
        _rm = this;
        _executor.setRemoveOnCancelPolicy(true);
        _executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        _executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public static ResourceManager getInstance() {
        return _rm; 
    };

    public ResourceManager clone() {
        _system.reset();
        final ResourceManager rm =  new ResourceManager();
        rm._system = _system;
        _rm = rm;
        return rm;
    }

    public void start() {
        register(Thread.currentThread());
        _finished.set(false);
        
        final int core = Config.strategy().getCorePoolSize();
        final int max = Config.strategy().getMaxPoolSize();
        final long defFrequency = Math.max(100, 10_000 / max);
        final boolean scalable = core < max;
        final long repeatFrequency = scalable ? defFrequency : 10_000; 

        // Collect resource-views
        _system.collect();

        final Runnable cycle = () -> {
            final long start = System.nanoTime();
            try {
                _rm.dispatch(false);
                _rm._system.scale();
                _rm.resumeWhenMemoryFree();
            } catch (Exception e) {
            } finally {
                DUUIComposer.totalrm.getAndAdd(System.nanoTime() - start);
            }
        };

        _executor.scheduleWithFixedDelay(cycle, 0, repeatFrequency, TimeUnit.MILLISECONDS);
    }

    public void setCasPoolMemoryThreshhold(long threshold) {
        _system.setCasPoolMemoryThreshhold(threshold);
    }

    public void setJvmMemoryThreshold(double percentage) {
        _system.setJvmMemoryThreshold(percentage);
    }

    public synchronized static void register(Thread thread, boolean worker) {
        _rm._system.register(thread, worker);
    }
   
    public static void register(Thread thread) {
        register(thread, false);
    }

    public synchronized static void register(IDUUIResource resource) {
        _rm._system.register(resource);
    }

    public void finishManager() throws InterruptedException {
        _finished.set(true);
        _executor.shutdown();
        _executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        _executor.shutdownNow();
    }

    void dispatch(final boolean started) {   
        final ResourceViews data = _system.update(); 
        if (Config.monitor() != null && Config.monitor() instanceof IDUUIResourceProfiler) {
            final IDUUIResourceProfiler monitor = (IDUUIResourceProfiler) Config.monitor();
            monitor.addMeasurements(data, started);
        }
        if (Config.storage() != null && Config.storage() instanceof IDUUIResourceProfiler) {
            final IDUUIResourceProfiler storage = (IDUUIResourceProfiler) Config.storage();
            storage.addMeasurements(data, started);
        }
    }

    public void initialiseCasPool(PoolStrategy strategy, TypeSystemDescription ts) throws UIMAException {
        _casPool = new CasPool(strategy, ts);
    }

    public JCas takeCas() throws InterruptedException {
        if (_casPool == null)
            throw new RuntimeException(
                "[DUUIResourceManager] JCas-Pool was not initialized.");

        waitIfMemoryCritical();
        JCas jc = _casPool.takeCas();
        _batchRead.set(false);
        return jc; 
    }

    public void returnCas(JCas jc) {
        if (_casPool == null)
            throw new RuntimeException(
                "[DUUIResourceManager] CAS-Pool was not initialized.");

        final long used = _system._usedBytes;
        final long thresholdBytes = _system._thresholdBytes;
        System.out.printf(
            "CAS returned! Used: %s | Threshhold: %s %n", formatb(used), formatb(thresholdBytes));
        jc.reset();
        _casPool.returnCas(jc);
    }

    public ByteArrayOutputStream takeByteStream() throws InterruptedException {
        if (_casPool == null) 
            return new ByteArrayOutputStream(3*1024*1024);

        return _casPool.takeStream();
    }

    public void returnByteStream(ByteArrayOutputStream bs) {
        if (_casPool == null) return; 

        bs.reset();
        _casPool.returnByteStream(bs);
    }
    
    public int getBorrowedCASCount() {
        if (_casPool == null) return 0;
        return _casPool.borrowedCount();
    }

    public boolean isBatchReadIn() {
        if (_casPool == null) return false;
        return _batchRead.get() || _casPool.isEmpty();
    }

    public void setBatchReadIn(boolean batchRead) {
        _batchRead.set(batchRead);
    }

    public void waitIfMemoryCritical() throws InterruptedException {
        if (!_system.isMemoryCritical() || !_finished.get()) 
            return;

        memoryLock.lock();
        try {
            memoryCritical = true;
            _batchRead.set(memoryCritical);
        } finally {
            memoryLock.unlock();
        }
        final long used = _system._usedBytes;
        final long thresholdBytes = _system._thresholdBytes;
        System.out.printf(
            "MEMORY CRITICAL! Used: %s | Threshhold: %s %n", formatb(used), formatb(thresholdBytes));
        
        memoryLock.lock();
        try {
            while (memoryCritical) unpaused.await();
        } finally {
            memoryLock.unlock();
        }
    }

    public void resumeWhenMemoryFree() {
        if (_casPool == null || !_finished.get()) 
            return;

        memoryLock.lock();
        try {
            if (!_system.isMemoryCritical()) {
                memoryCritical = false;
                unpaused.signalAll();
            } else {
                _casPool.reduceExcessResources();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            memoryLock.unlock();
        }
    }

    class CasPool {
        final BlockingQueue<JCas> _casPool;
        final Collection<JCas> _casMonitorPool; 
        final BlockingQueue<ByteArrayOutputStream> _bytestreams;
        final Supplier<JCas> _casSupplier; 
        final Supplier<ByteArrayOutputStream> _streamSupplier; 
        final PoolStrategy _strategy;
        final AtomicInteger _currCasPoolSize; 
        final AtomicInteger _currStreamPoolSize;
        final AtomicInteger _maxPoolSize; 
        final ByteArrayOutputStream test = new ByteArrayOutputStream(10*1024*1024);


        CasPool(PoolStrategy strategy, TypeSystemDescription desc) throws UIMAException {
            _strategy = strategy;
            _casPool = _strategy.instantiate(JCas.class);
            _bytestreams = _strategy.instantiate(ByteArrayOutputStream.class);
            _casMonitorPool = ConcurrentHashMap.newKeySet(_strategy.getCorePoolSize());
            _maxPoolSize = new AtomicInteger(_casPool.remainingCapacity());
            
            final JCas exJcas = JCasFactory.createJCas(desc); 
            TypeSystem ts = exJcas.getTypeSystem();
            _casSupplier = () -> 
                {
                    try {
                        return CasCreationUtils
                            .createCas(ts, null, null, null)
                            .getJCas();
                    } catch (CASException | ResourceInitializationException e) {
                        e.printStackTrace(); return null; 
                    }
                }; 
           
            int initialPoolSize = _casPool.remainingCapacity() == Integer.MAX_VALUE ?
                _strategy.getMaxPoolSize() : _casPool.remainingCapacity();
            boolean casCreationFailed = Stream.generate(_casSupplier)
                .limit(initialPoolSize)
                .peek(_casMonitorPool::add)
                .peek(_casPool::add)
                .anyMatch(Predicate.isEqual(null));

            assert casCreationFailed : 
                new RuntimeException("[DUUIResourceManager] Exception occured while populating JCas pool.");
            _currCasPoolSize = new AtomicInteger(initialPoolSize);
            
            _streamSupplier = () -> new ByteArrayOutputStream(exJcas.size());
            Stream.generate(_streamSupplier)
                .limit(initialPoolSize)
                .forEach(_bytestreams::add);
            _currStreamPoolSize = new AtomicInteger(initialPoolSize);
            
            // System.out.printf(
            //     "JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d \nMEMORY USED:      %s \nMEMORY THRESHOLD: %s %n", 
            //     _maxPoolSize.get(), initialPoolSize, _casPool.size(), formatb(_system._usedBytes), formatb(_system._thresholdBytes));
        }

        int borrowedCount() {
            return _currCasPoolSize.get() - _casPool.size();
        }

        boolean isEmpty() {
            return _maxPoolSize.get() <= _currCasPoolSize.get() - _casPool.size();
        }

        void resetMaxPoolSize() {
            _maxPoolSize.set(_strategy.getInitialQueueSize()); 
        }

        void setMaxPoolSize() {
            _maxPoolSize.set(_currCasPoolSize.get() - _casPool.size()); 
        }

        void reduceExcessResources() throws InterruptedException {
            while (_casPool.size() > 0 && _system.isMemoryCritical()) {
                JCas jc = _casPool.poll();
                if (jc == null) break;
                _casMonitorPool.remove(jc);
                jc = null;
                _bytestreams.poll();
                _currCasPoolSize.decrementAndGet();
                _currStreamPoolSize.decrementAndGet();
                System.gc();
            }
            
            if (_casMonitorPool.size() == 0) {
                // TODO: destroy resources? 
                _rm.finishManager();
                unpaused.signalAll();
                throw new RuntimeException("[DUUIResourceManager] Memory threshold still crossed after freeing all CAS-Objects.");
            }
        }
        
        void returnCas(JCas jc) {
            // System.out.printf(
            //     "JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d \nMEMORY USED (CAS-Pool/JVM):      %s / %s \nMEMORY THRESHOLD: %s %n", 
            //     _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size(), formatb(_system._usedBytes), formatb(_system._systemView.memory_used), formatb(_system._thresholdBytes));
            _casPool.offer(jc);

        }

        void returnByteStream(ByteArrayOutputStream bs) {
            _bytestreams.offer(bs);
        }

        JCas takeCas () throws InterruptedException {
            // System.out.printf(
            //     "JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d \nMEMORY USED (CAS-Pool/JVM):      %s / %s \nMEMORY THRESHOLD: %s %n", 
            //     _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size(), formatb(_system._usedBytes), formatb(_system._systemView.memory_used), formatb(_system._thresholdBytes));
            return take(_casPool, _casSupplier, _currCasPoolSize);
        }

        ByteArrayOutputStream takeStream() throws InterruptedException {
            return take(_bytestreams, _streamSupplier, _currStreamPoolSize);
        }

        <T> T take(BlockingQueue<T> pool, Supplier<T> generator, AtomicInteger totalNumberOfItems) throws InterruptedException {
            int borrowedItems = totalNumberOfItems.get() - pool.size();

            if (borrowedItems >= _maxPoolSize.get())
                return pool.take();

            T resource = pool.poll();
            
            if (resource != null)
                return resource; 

            if (borrowedItems < _maxPoolSize.get()) {
                T t = generator.get();
                if (pool.offer(t)) {
                    totalNumberOfItems.incrementAndGet();
                    if (t instanceof JCas) {
                        JCas jc = (JCas) t;
                        _casMonitorPool.add(jc);
                    }
                }
            }

            return pool.take(); 
        }

        long getJCasMemoryConsumption() {
            long sum = 0l;
            for (JCas jc : _casMonitorPool) {
                sum += jc.size();
            } 
            return sum;
        }

    }

    class SystemResourceViews implements ResourceViews {

        final Runtime _runtime = Runtime.getRuntime();

        // Resoucre Views
        PipelineProgress _pipelineProgress = null;
        DockerDriverView _dockerDriverView = null;
        // TODO: DockerSwarmView, ... 

        final SystemView _systemView = new SystemView();
        final List<IDUUIResource> _resources = new ArrayList<>(); 
        final Map<Class<? extends IDUUIResource>, ResourceView> _resourceStats = new IdentityHashMap<>(_resources.size());

        long _thresholdBytes = (long) (Runtime.getRuntime().maxMemory() * 0.1);
        long _usedBytes = 0L; 
        AtomicBoolean _poolSizeSet = new AtomicBoolean(false);

        public void register(IDUUIResource resource) {
            _resources.add(resource);
        }

        public void register(Thread thread, boolean worker) {
            _systemView.register(thread, worker);
        }

        void reset() {
            _pipelineProgress = null; 
            // _dockerDriverView = null; Drivers should persist through multiple Composer.shutdown() calls.
            _resources.removeIf(r -> !(r instanceof IDUUIDriver));
            _resourceStats.clear();
            _poolSizeSet.set(false);
            _systemView._tvs.clear();
        }

        void setCasPoolMemoryThreshhold(long thresholdBytes) {
            _thresholdBytes = thresholdBytes;
        }

        void setJvmMemoryThreshold(double percentage) {
            _systemView.setMemoryThreshold(percentage);
        }

        boolean isMemoryCritical() {
            if (_casPool == null) 
                return false; 

            _usedBytes = _casPool.getJCasMemoryConsumption();
            boolean critical = _usedBytes > (_thresholdBytes + 25*1024*1024) || _systemView.isMemoryCritical();

            // If threshhold has been set once, but is back below again.
            if (!critical) {
                _casPool.resetMaxPoolSize();
            }else if (critical) {
                _casPool.setMaxPoolSize();
            }
                
            return critical;
        }
        
        public void scale() {
            _resources.forEach(r ->
            {
                try {
                    r.scale(_system);
                } catch (Exception e) {
                    System.out.printf("[DUUIResourceManager] Error scaling resource %s%n", r);
                    e.printStackTrace();
                }
            });
        }

        @Override
        public ResourceViews update() {
            _systemView.update();
            if (_pipelineProgress != null) _pipelineProgress.update();
            // _dockerDriverView.update(); TODO: Too slow to do in a loop. Maybe for verification.
            
            return this;
        }

        // COLLECTION METHODS
        ResourceViews collect() {
            collectResourcesData();
            _systemView.update();
            return this; 
        }

        Map<Class<? extends IDUUIResource>, ResourceView> collectResourcesData() {
            for (IDUUIResource resource : _resources) {
                try {
                    ResourceView resourceView = resource.collect();
                    if (resourceView == null) continue; 

                    if (resourceView instanceof PipelineProgress) {
                        _pipelineProgress = (PipelineProgress) resourceView; 
                    } else if (resourceView instanceof DockerDriverView) {
                        _dockerDriverView = (DockerDriverView) resourceView;
                    } else {
                        _resourceStats.put(resource.getClass(), resourceView);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "[DUUIResourceManager] Error collecting resource stats: %s%n%s%n",
                        e, e.getMessage());
                }
            }

            return _resourceStats; 
        }

        // RESOURCE VIEWS
                
        @Override
        public DockerDriverView getDockerDriverView() {
            return _dockerDriverView;
        }
        
        @Override
        public PipelineProgress getComponentProgress() {
            return _pipelineProgress;
        }

        @Override 
        public HostUsage getHostUsage() {
            return _systemView;
        }

        @Override 
        public HostConfig getHostConfig() {
            return _systemView._config;
        }

        @Override
        public Map<Class<? extends IDUUIResource>, ResourceView> getResourceViews() {
            return _resourceStats;
        }
    }

    class SystemView implements HostUsage {
        class ThreadView implements HostThreadView {
            final Thread th;
            final long id;
            final boolean worker;
            String name;
            String state;
            long wait_time; // milliseconds
            long block_time; // milliseconds
            long cpu_time; // milliseconds
            long memory_usage; 

            ThreadView(Thread thread, boolean worker) {
                this.id = thread.getId();
                this.th = thread;
                this.worker = worker;
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getState() {
                return state;
            }

            @Override
            public long getWaitedTime() {
                return wait_time;
            }

            @Override
            public long getBlockedTime() {
                return block_time;
            }
            
            @Override
            public long getCpuTime() {
                return cpu_time;
            }

            @Override
            public long getMemoryUsage() {
                return memory_usage;
            }

            boolean isDead() {
                return th.getState().equals(Thread.State.TERMINATED);
            }

            @Override
            public void update() {
                long memoryUsage = -1L;
                if (_threads instanceof com.sun.management.ThreadMXBean) {
                    com.sun.management.ThreadMXBean threadsSun = 
                        (com.sun.management.ThreadMXBean) _threads;
                    memoryUsage = threadsSun.getThreadAllocatedBytes(id);
                }
                this.name = th.getName();
                ThreadInfo info = _threads.getThreadInfo(id);
                this.state = info.getThreadState().toString();
                this.wait_time = info.getWaitedTime();
                this.block_time = info.getBlockedTime();
                this.cpu_time = TimeUnit.NANOSECONDS.toMillis(_threads.getThreadCpuTime(id));
                this.memory_usage = memoryUsage;
            }
        }

        final OperatingSystemMXBean _os = ManagementFactory.getOperatingSystemMXBean();
        final ThreadMXBean _threads = ManagementFactory.getThreadMXBean();
        final MemoryMXBean _memory = ManagementFactory.getMemoryMXBean();
        final Runtime _runtime = Runtime.getRuntime();

        final SystemConfigView _config = new SystemConfigView();
        final ConcurrentHashMap<Long, ThreadView> _tvs;

        double cpu_load_average = -1.f;
        double system_cpu_load = -1.f; // percent [0.0, 1.0]
        double jvm_cpu_load = -1.f; // percent [0.0, 1.0]
        long jvm_cpu_time = -1L; // milliseconds

        long memory_used = -1L;
        long memory_total = -1L;
        long system_memaory_used = -1L;

        double memory_threshold = 0.8;

        public SystemView (){
            _tvs = new ConcurrentHashMap<>(Config.strategy().getMaxPoolSize() * 2);

            try {
                _threads.setThreadContentionMonitoringEnabled(true);
                _threads.setThreadCpuTimeEnabled(true);

                if (_threads instanceof com.sun.management.ThreadMXBean) {
                    final com.sun.management.ThreadMXBean threadsSun = 
                        (com.sun.management.ThreadMXBean) _threads;
        	        threadsSun.setThreadAllocatedMemoryEnabled(true);
                }
            } catch (Exception e) {
                System.out.println("[DUUIResourceManager] Thread monitoring limited.");
            }
        }

        public void register(Thread thread, boolean worker) {
            if (!_tvs.containsKey(thread.getId()))
                _tvs.putIfAbsent(thread.getId(), new ThreadView(thread, worker));
        }

        public void setMemoryThreshold(double percentage) {
            memory_threshold = percentage;
        }

        public boolean isMemoryCritical() {
            return _config.jvm_max_memory * memory_threshold < memory_used;
        }

        @Override
        public void update() {
            if (_os instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean osSun = (com.sun.management.OperatingSystemMXBean) _os;
                cpu_load_average = osSun.getSystemLoadAverage();
                system_cpu_load = osSun.getSystemCpuLoad();
                jvm_cpu_time = TimeUnit.NANOSECONDS.toMillis(osSun.getProcessCpuTime());
                jvm_cpu_load = osSun.getProcessCpuLoad();
                system_memaory_used = osSun.getTotalPhysicalMemorySize() - osSun.getFreePhysicalMemorySize();
            }
            memory_used = _runtime.totalMemory() - _runtime.freeMemory();
            memory_total = _runtime.totalMemory();

            _tvs.entrySet().removeIf(e -> e.getValue().isDead());
            _tvs.values().forEach(ThreadView::update);
        }

        @Override
        public Iterable<? extends HostThreadView> getThreadViews() {
            return _tvs.values();
        }

        @Override
        public double getSystemCpuLoad() {
            return system_cpu_load;
        }

        @Override
        public double getJvmCpuLoad() {
            return jvm_cpu_load;
        }

        @Override
        public long getJvmCpuTime() {
            return jvm_cpu_time;
        }

        @Override
        public long getHostMemoryUsage() {
            return system_memaory_used;
        }

        @Override
        public long getHeapMemoryUsage() {
            return memory_used;
        }

        @Override
        public long getHeapMemoryTotal() {
            return memory_total;
        }

        
        class SystemConfigView implements HostConfig {
            final int processors;
            final String arch;
            final String os_version;
            final String os_name;
            final String jvm_vendor;
            final long jvm_max_memory;
            final long system_memory_total;

            SystemConfigView() {
                processors = _os.getAvailableProcessors();
                arch = _os.getArch();
                os_version = _os.getVersion();
                os_name = _os.getName();
                jvm_vendor = ManagementFactory.getRuntimeMXBean().getVmVendor();
                jvm_max_memory = _runtime.maxMemory();

                if (_os instanceof com.sun.management.OperatingSystemMXBean) {
                    com.sun.management.OperatingSystemMXBean osSun = (com.sun.management.OperatingSystemMXBean) _os;
                    system_memory_total = osSun.getTotalPhysicalMemorySize();
                } else system_memory_total = -1;

            }

            @Override
            public String getOSName() {
                return os_name; 
            }

            @Override
            public String getJVMVendor() {
                return jvm_vendor;
            }

            @Override
            public int getCASPoolSize() {
                if (_casPool == null)
                    return Config.strategy().getInitialQueueSize();
                return _casPool._maxPoolSize.get(); // Can change for memory control.
            }

            @Override
            public long getJVMMaxMemory() {
                return jvm_max_memory;
            }

            @Override
            public long getHostMemoryTotal() {
                return system_memory_total;
            }

            @Override
            public int getAvailableProcessors() {
                return processors;
            }
        }
    }
    
    public static interface ResourceView {
        default void update() {};
    }

    public static interface ResourceViews {

        ResourceViews update();

        PipelineProgress getComponentProgress();

        DockerDriverView getDockerDriverView();

        HostConfig getHostConfig();

        HostUsage getHostUsage();

        Map<Class<? extends IDUUIResource>, ResourceView> getResourceViews();
    }

    public static interface PipelineProgress extends ResourceView {

        int getComponentPoolSize(String uuid);

        long getRemainingNanos();

        double getLevelProgress();

        double getComponentProgress(String uuid);

        boolean isCompleted(String uuid);
        
        boolean hasShutdown();

        default boolean isBatchReadIn() {
            return _rm.isBatchReadIn();
        }

        int getPipelineLevel(String uuid);

        int getPipelineDepth();

        int getNextLevel();

        int getCurrentLevel();

        int getLevelSize(int level);

        int getLevelSize(int level, Class<? extends IDUUIDriver> filter);

        int getLevelSize(int level, Class<? extends IDUUIDriver> ...filters);
    }

    public static interface HostConfig extends ResourceView {

        long getJVMMaxMemory();

        long getHostMemoryTotal();

        int getAvailableProcessors();

        int getCASPoolSize();

        String getOSName();

        String getJVMVendor();
    }

    public static interface HostUsage extends ResourceView {

        double getSystemCpuLoad();

        double getJvmCpuLoad();

        long getJvmCpuTime();

        long getHostMemoryUsage();

        long getHeapMemoryUsage();

        long getHeapMemoryTotal();

        Iterable<? extends HostThreadView> getThreadViews(); 

        // int calculateDynamicPoolsize();
    }

    public static interface HostThreadView extends ResourceView {

        String getName();
        
        String getState();

        long getWaitedTime();

        long getBlockedTime();
        
        long getCpuTime();

        long getMemoryUsage();
    }
}
