package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.HostUsage;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.SystemView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUISimpleMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.IDUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.PoolStrategy;

public class ResourceManager extends Thread {

    static final List<IDUUIResource> _resources = new ArrayList<>(); 
    static final AtomicBoolean _finished = new AtomicBoolean(false);
    static final Set<Thread> _activeThreads = ConcurrentHashMap.newKeySet(20);
    static final Map<Long, Thread> _workerThreads = new ConcurrentHashMap<>(20);
    static final CountDownLatch finishLock = new CountDownLatch(1);

    CasPool _casPool;     

    SystemResourceStatistics _system; 
    boolean memoryCritical = false;
    final ReentrantLock memoryLock = new ReentrantLock();
    final Condition unpaused = memoryLock.newCondition();
    final AtomicBoolean _batchRead = new AtomicBoolean(false);

    static final ConcurrentLinkedQueue<Runnable> _tasks = new ConcurrentLinkedQueue<>(); 
    public DUUISimpleMonitor _monitor = null; 
    static ResourceManager _rm = new ResourceManager();
    

    ResourceManager() {
        super("DUUIResourceManager");
        // this.setDaemon(true);
        
        _system = new SystemResourceStatistics(0.2, -1L);
    }

    public ResourceManager(double memoryThreshholPercentage, long memoryThreshholdBytes) {
        super("DUUIResourceManager");

        _system = new SystemResourceStatistics(memoryThreshholPercentage, memoryThreshholdBytes);
        _rm = this;
    }

    public static ResourceManager getInstance() {
        return _rm; 
    };

    public void withMonitor(IDUUIMonitor monitor) {
        _monitor = (DUUISimpleMonitor) monitor;
    }

    public void start() {
        _activeThreads.add(this);
        super.start();
    }

    public synchronized static void register(Thread thread, boolean worker) {
        _activeThreads.add(thread);
        if (worker) _workerThreads.put(thread.getId(), thread);
    }
   
    public synchronized static void register(Thread thread) {
        _activeThreads.add(thread);
    }

    public synchronized static void register(IDUUIResource resource) {
        _resources.add(resource);
    }

    @Override
    public void run() {
        try {
            // Collection phase
            dispatchResourceViews();
            
            while (! _finished.get()) {
                dispatchHostView();

                Thread.sleep(1000);
                
                //Main thread memory lock
                resumeWhenMemoryFree();            

                // Scaling phase
                scale();
                
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        // } catch (Exception e) {
        } finally {
            finishLock.countDown();
        }
    }

    public void finishManager() throws InterruptedException {
        _finished.set(true);
        if (!Thread.interrupted())
            finishLock.await();
    }

    void scale() {
        _resources.forEach(r ->
        {
            try {
                r.scale(_system);
            } catch (Exception e) {
                System.out.printf(
                    "[DUUIResourceManager] Error scaling resource %s%n"
                    , r
                );
                e.printStackTrace();
            }
        });
    }

    public boolean isBatchReadIn() {
        return _batchRead.get() || _casPool._casPool.size() == 0;
    }

    public void setBatchReadIn(boolean batchRead) {
        _batchRead.set(batchRead);
    }
    
    public void waitIfMemoryCritical() {
        if (!_system.isMemoryCritical()) 
            return;

        memoryLock.lock();
        try {
            memoryCritical = true;
        } finally {
            memoryLock.unlock();
        }
        long used = _system._usedBytes;
        long thresholdBytes = _system._thresholdBytes;
        System.out.printf(
            "MEMORY CRITICAL! Used: %d | Threshhold: %d %n", used, thresholdBytes);
        
        memoryLock.lock();
        try {
            while (memoryCritical) unpaused.await();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            memoryLock.unlock();
        }
    }

    public void resumeWhenMemoryFree() {
        memoryLock.lock();
        try {
            if (!_system.isMemoryCritical()) {
                // System.out.printf(
                //     "MEMORY SAFE! Used: %d | Threshhold: %d %n", 
                //     used, thresholdBytes);
                memoryCritical = false;
                unpaused.signalAll();
            }
        } finally {
            memoryLock.unlock();
        }
    }

    public void initialiseCasPool(PoolStrategy strategy, TypeSystemDescription ts) throws UIMAException {
        _casPool = new CasPool(strategy, ts);
    }

    public JCas takeCas() throws InterruptedException {
        if (_casPool == null)
            throw new RuntimeException(
                "[DUUIResourceManager] JCas-Pool was not initialized.");

        if (_casPool.poolFullAndLimited())
            _batchRead.set(true);
        waitIfMemoryCritical();
        JCas jc = _casPool.takeCas();
        _batchRead.set(false);
        return jc; 
    }

    public void returnCas(JCas jc) {
        if (_casPool == null)
            throw new RuntimeException(
                "[DUUIResourceManager] JCas-Pool was not initialized.");
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

    private void dispatchResourceViews() {
        final Map<Class<? extends IDUUIResource>, ResourceView> data = 
            _system.collectResourcesData();
        // dispatch(,
        //     DUUISimpleMonitor.V1_MONITOR_RESOURCE_INFO);
    }

    private void dispatchHostView() {
        final ResourceViews data = _system.collect();
        // dispatch(_system.collect(), 
        //     DUUISimpleMonitor.V1_MONITOR_SYSTEM_DYNAMIC_INFO);
    }

    void dispatch(Map<String, Object> resourceData, String type) {    
        if (_monitor == null)
            return;  
        try {
            _monitor.sendUpdate(resourceData, type);
        } catch (IOException e) {
            System.out.println(
                "[DUUIResourceManager] Error sending stats to monitor.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            this.interrupt();
        }
    }

    class CasPool {
        final BlockingQueue<JCas> _casPool;
        final List<JCas> _casMonitorPool; 
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
            _casMonitorPool = new ArrayList<>(_strategy.getCorePoolSize());
            _maxPoolSize = new AtomicInteger(_casPool.remainingCapacity());
            
            JCas exJcas = JCasFactory.createJCas(desc); 
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
                _strategy.getCorePoolSize() : _casPool.remainingCapacity();
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
            
            System.out.printf(
                "JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d \nMEMORY USED:      %d \nMEMORY THRESHOLD: %d %n", 
                _maxPoolSize.get(), initialPoolSize, _casPool.size(), _system._usedBytes, _system._thresholdBytes);
        }

        boolean poolFullAndLimited() {
            return (_maxPoolSize.get() < Integer.MAX_VALUE) && _casPool.size() == 0;
        }

        void resetMaxPoolSize() {
            if (_casPool.remainingCapacity() == Integer.MAX_VALUE)
                _maxPoolSize.set(Integer.MAX_VALUE);
        }

        void setMaxPoolSize() {
            _maxPoolSize.set(_casMonitorPool.size()); 
        }
        
        void returnCas(JCas jc) {
            // System.out.printf("JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size());
            _casPool.offer(jc);

        }

        void returnByteStream(ByteArrayOutputStream bs) {
            // System.out.printf("BYTESTREAM QUEUE CAPACITY: %d | #BYTESTREAM: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currStreamPoolSize.get(), _bytestreams.size());
            _bytestreams.offer(bs);
        }

        JCas takeCas () throws InterruptedException {
            System.out.printf(
                "JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d \nMEMORY USED:      %d \nMEMORY THRESHOLD: %d %n", 
                _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size(), _system._usedBytes, _system._thresholdBytes);
            return take(_casPool, _casSupplier, _currCasPoolSize);
        }

        ByteArrayOutputStream takeStream() throws InterruptedException {
            // System.out.printf("BYTESTREAM QUEUE CAPACITY: %d | #BYTESTREAM: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currStreamPoolSize.get(), _bytestreams.size());
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
                        // System.out.printf("JCAS NEW ELEMENT ADDED: QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d %n", _maxPoolSize.get(), poolSize.get(), _casPool.size());
                    } else {
                        // System.out.printf("BYTESTREAM NEW ELEMENT ADDED: QUEUE CAPACITY: %d | #BYTESTREAM: %d | #RESERVED: %d %n", _maxPoolSize.get(), poolSize.get(), _bytestreams.size());
                    }
                }
            }

            return pool.take(); 
        }

        long getJCasMemoryConsumption() {
            return _casMonitorPool.stream()
                    .mapToInt(JCas::size)
                    .sum();
        }

    }

    class SystemResourceStatistics implements ResourceViews {

        final Runtime _runtime = Runtime.getRuntime();

        // Resoucre Views
        PipelineProgress _pipelineProgress = null;
        final SystemView _systemView = new SystemView(_activeThreads);
        final Map<Class<? extends IDUUIResource>, ResourceView> _resourceStats = new IdentityHashMap<>(_resources.size());
        final Set<IDUUIResource<ResourceView>> _skipCollection = new HashSet<>(16);

        final long _thresholdBytes;
        long _usedBytes = 0L; 
        AtomicBoolean _poolSizeSet = new AtomicBoolean(false);
        
        public SystemResourceStatistics(double memoryThreshhold, long thresholdBytes) {
            if (memoryThreshhold == -1 && thresholdBytes == -1) 
                _thresholdBytes = _runtime.totalMemory();
            else if (memoryThreshhold != -1)
                _thresholdBytes = Math.round(memoryThreshhold *_runtime.maxMemory());
            else 
                _thresholdBytes = thresholdBytes;

            System.out.printf("[DUUIResourceManager] MEMORY THRESHOLD SET: %d%n", _thresholdBytes);
        }

        boolean isMemoryCritical() {
            if (_casPool == null) 
                return false; 

            _usedBytes = _casPool.getJCasMemoryConsumption();
            boolean critical = _usedBytes > _thresholdBytes;

            // If threshhold has been set once, but is back below again.
            if (!critical) {
                _poolSizeSet.set(false);
                _casPool.resetMaxPoolSize();
            }

            // if threshhold has been set, and is still critical. 
            if (_poolSizeSet.get())
                return false;

            // set threshhold
            if (critical) {
                _casPool.setMaxPoolSize();
                _poolSizeSet.set(true);
                _batchRead.set(true);
            }
                
            return critical;
        }

        // RESOURCE VIEWS

        @Override
        public void update() {
            _systemView.update();
            _pipelineProgress.update();
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
        // COLLECTION METHODS

        ResourceViews collect() {
            update();
            return this; 
        }

        Map<Class<? extends IDUUIResource>, ResourceView> collectResourcesData() {
            for (IDUUIResource resource : _resources) {
                if (_skipCollection.contains(resource)) continue;
                    
                try {
                    ResourceView resourceView = resource.collect();
                    if (resourceView == null) continue; 

                    if (resourceView instanceof PipelineProgress) {
                        _pipelineProgress = (PipelineProgress) resourceView; 
                    } else {
                        _resourceStats.put(resource.getClass(), resourceView);
                    }
                    _skipCollection.add(resource);
                } catch (Exception e) {
                    System.out.printf(
                        "[DUUIResourceManager] Error collecting resource stats: %s%n%s%n",
                        e, e.getMessage());
                }
            }

            return _resourceStats; 
        }
        
        // public int calculateDynamicPoolsize() {
        //     long[] cumulatedCpuTime = {0L};
        //     long[] cumulatedWaitTime = {0L};

        //     // _threadStats.keySet().stream()
        //     // .map(Long::valueOf)
        //     // .filter(_workerThreads::containsKey)
        //     // .map(String::valueOf)
        //     // .forEach(threadId -> {
        //     //     cumulatedWaitTime[0] += (long) _threadStats
        //     //         .get(threadId).get("thread_total_wait_time");
        //     //     cumulatedCpuTime[0] += (long) _threadStats
        //     //         .get(threadId).get("thread_cpu_time");
        //     // });


        //     double blockingFactor;
        //     // System.out.println("CUMULATED CPU TIME: " + cumulatedCpuTime[0]);
        //     // System.out.println("CUMULATED WAIT TIME: " + cumulatedWaitTime[0]);
        //     if (cumulatedCpuTime[0] == 0L || cumulatedWaitTime[0] == 0L)
        //         blockingFactor = 0.5;
        //     else
        //         blockingFactor = cumulatedCpuTime[0] / cumulatedWaitTime[0];

        //     return (int) Math.round(getAvailableProcessors() / (1 - blockingFactor));
        // }
    }
    
    public static interface ResourceView {
        default void update() {};
    }

    public static interface ResourceViews extends ResourceView {

        PipelineProgress getComponentProgress();

        HostConfig getHostConfig();

        HostUsage getHostUsage();

        Map<Class<? extends IDUUIResource>, ResourceView> getResourceViews();
    }

    public static interface PipelineProgress extends ResourceView {

        int getComponentPoolSize(String uuid);

        long getAcceleration();

        double getLevelProgress();

        double getComponentProgress(String uuid);

        boolean isCompleted(String uuid);

        default boolean isBatchReadIn() {
            return _rm.isBatchReadIn();
        }

        int getPipelineLevel(String uuid);

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

        String getOSName();

        String getJVMVendor();
    }

    public static interface HostUsage extends ResourceView {

        double getSystemCpuLoad();

        double getJvmCpuLoad();

        long getHostMemoryUsage();

        long getHeapMemoryUsage();

        long getHeapMemoryTotal();

        // int calculateDynamicPoolsize();
    }

    static interface HostThreadView extends ResourceView {
        long getWaitedTime();

        long getBlockedTime();
        
        long getCpuTime();

        long getMemoryUsage();
    }

    class SystemView implements HostUsage {
        class ThreadView implements HostThreadView {
            final Thread th;
            final ThreadInfo info;
            final long id;
            String name;
            String state;
            long wait_time; // milliseconds
            long block_time; // milliseconds
            long cpu_time; // nanoseconds
            long memory_usage; 

            ThreadView(Thread thread) {
                this.id = thread.getId();
                this.th = thread;
                this.info = _threads.getThreadInfo(id);
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

            @Override
            public void update() {
                long id = th.getId();

                long memoryUsage = -1L;
                if (_threads instanceof com.sun.management.ThreadMXBean) {
                    com.sun.management.ThreadMXBean threadsSun = 
                        (com.sun.management.ThreadMXBean) _threads;
                    memoryUsage = threadsSun.getThreadAllocatedBytes(id);
                }
                this.name = th.getName();
                this.state = info.getThreadState().toString();
                this.wait_time = info.getWaitedTime();
                this.block_time = info.getBlockedTime();
                this.cpu_time = TimeUnit.NANOSECONDS.toMillis(_threads.getThreadCpuTime(id));
                this.memory_usage = memoryUsage;
            }
        }

        class SystemConfigView implements HostConfig {
            final int processors;
            final String arch;
            final String os_version;
            final String os_name;
            final String jvm_vendor;
            final long jvm_max_memory;
            final long system_memaory_total;

            SystemConfigView() {
                processors = _os.getAvailableProcessors();
                arch = _os.getArch();
                os_version = _os.getVersion();
                os_name = _os.getName();
                jvm_vendor = ManagementFactory.getRuntimeMXBean().getVmVendor();
                jvm_max_memory = _runtime.maxMemory();

                if (_os instanceof com.sun.management.OperatingSystemMXBean) {
                    com.sun.management.OperatingSystemMXBean osSun = (com.sun.management.OperatingSystemMXBean) _os;
                    system_memaory_total = osSun.getTotalPhysicalMemorySize();
                } else system_memaory_total = -1;

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
            public long getJVMMaxMemory() {
                return jvm_max_memory;
            }

            @Override
            public long getHostMemoryTotal() {
                return system_memaory_total;
            }

            @Override
            public int getAvailableProcessors() {
                return processors;
            }
        }

        final OperatingSystemMXBean _os = ManagementFactory.getOperatingSystemMXBean();
        final ThreadMXBean _threads = ManagementFactory.getThreadMXBean();
        final MemoryMXBean _memory = ManagementFactory.getMemoryMXBean();
        final Runtime _runtime = Runtime.getRuntime();

        final SystemConfigView _config = new SystemConfigView();
        final Map<Long, ThreadView> _threadviews;
        final Collection<ThreadView> _threadviewsSet;
        final Set<Thread> _th; 

        double cpu_load_average = -1.f;
        double system_cpu_load = -1.f; // percent [0.0, 1.0]
        double jvm_cpu_load = -1.f; // percent [0.0, 1.0]
        long jvm_cpu_time = -1L; // nanoseconds

        long memory_used = -1L;
        long memory_total = -1L;
        long system_memaory_used = -1L;

        public SystemView (Set<Thread> activeThreads){
            _th = activeThreads;
            _threadviews = _th.stream()
                .map(ThreadView::new)
                .collect(Collectors.toMap(tv-> tv.id, Function.identity()));
            _threadviewsSet = _threadviews.values();

            try {
                _threads.setThreadContentionMonitoringEnabled(true);
                _threads.setThreadCpuTimeEnabled(true);
            } catch (Exception e) {
                System.out.println("[DUUIResourceManager] Thread monitoring limited.");
            }
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

        @Override
        public void update() {
            if (_os instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean osSun = (com.sun.management.OperatingSystemMXBean) _os;
                cpu_load_average = osSun.getSystemLoadAverage();
                system_cpu_load = osSun.getSystemCpuLoad();
                jvm_cpu_time = osSun.getProcessCpuTime();
                jvm_cpu_load = osSun.getProcessCpuLoad();
                system_memaory_used = osSun.getTotalPhysicalMemorySize() - osSun.getFreePhysicalMemorySize();
            }
            
            memory_used = _runtime.totalMemory() - _runtime.freeMemory();
            memory_total = _runtime.totalMemory();

            _th.stream()
                .filter(t -> !_threadviews.containsKey(t.getId()))
                .map(ThreadView::new)
                .forEach(tv -> _threadviews.putIfAbsent(tv.id, tv));
            _threadviewsSet.forEach(ThreadView::update);
        }
    }

}
