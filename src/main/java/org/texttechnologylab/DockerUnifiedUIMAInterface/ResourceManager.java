package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUISimpleMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.IDUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.PoolStrategy;

public class ResourceManager extends Thread {

    static final List<IDUUIResource> _resources = new ArrayList<>(); 
    static final AtomicBoolean _finished = new AtomicBoolean(false);
    static final HashSet<Thread> _activeThreads = new HashSet<>(20);
    static final Map<Long, Thread> _workerThreads = new ConcurrentHashMap<>(20);


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
            dispatch(_system.collectStaticData(), 
                DUUISimpleMonitor.V1_MONITOR_SYSTEM_STATIC_INFO);

            while (! _finished.get()) {
                Thread.sleep(500);
                
                //Main thread memory control
                resumeWhenMemoryFree();
        
                // Collection phase
                dispatchSystemDynamicInfo();
                dispatchResourceInfo();

                // Scaling phase
                scale();
                
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void finishManager() {
        _finished.set(true);
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
        return _batchRead.get();
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

    private void dispatchResourceInfo() {
        dispatch(_system.collectResourcesData(),
            DUUISimpleMonitor.V1_MONITOR_RESOURCE_INFO);
    }

    private void dispatchSystemDynamicInfo() {
        dispatch(_system.collect(), 
            DUUISimpleMonitor.V1_MONITOR_SYSTEM_DYNAMIC_INFO);
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
           
            boolean casCreationFailed = Stream.generate(_casSupplier)
                .limit(_strategy.getCorePoolSize())
                .peek(_casMonitorPool::add)
                .peek(_casPool::add)
                .anyMatch(Predicate.isEqual(null));

            assert casCreationFailed : 
                new RuntimeException("[DUUIResourceManager] Exception occured while populating JCas pool.");
            _currCasPoolSize = new AtomicInteger(_strategy.getCorePoolSize());
            
            _streamSupplier = () -> new ByteArrayOutputStream(exJcas.size());
            Stream.generate(_streamSupplier)
                .limit(_strategy.getCorePoolSize())
                .forEach(_bytestreams::add);
            _currStreamPoolSize = new AtomicInteger(_strategy.getCorePoolSize());
            
            System.out.printf(
                "JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d \nMEMORY USED:      %d \nMEMORY THRESHOLD: %d %n", 
                _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size(), _system._usedBytes, _system._thresholdBytes);
        }

        boolean poolFullAndLimited() {
            return (_maxPoolSize.get() < Integer.MAX_VALUE) && _casPool.size() == 0;
        }

        void resetMaxPoolSize() {
            if (_casPool.remainingCapacity() == Integer.MAX_VALUE)
                _maxPoolSize.set(Integer.MAX_VALUE);
        }

        void setMaxPoolSize() {
            if (_casPool.remainingCapacity() < Integer.MAX_VALUE)
                return;

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

            T resource = pool.poll(_strategy.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            
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
            // Consumer<JCas> serializer = (jc) -> {
            //     synchronized (jc) {
            //         try {
            //             Serialization.serializeCAS(jc.getCas(), test);
            //         } catch (Exception e) {
            //         }
            //     }
            // };

            // _casMonitorPool.stream()
            //     .findAny()
            //     .ifPresent(serializer);

            // long size;
            // if (test.size() > 0) {
            //     size = test.size() * _casMonitorPool.size();
            //     test.reset();
            //     return size;
            // } else {
                
            // }
        }

    }

    public static interface HostConfig {

        long getMemoryThreshhold();

        long getJVMMaxMemory();

        long getHostMemoryTotal();

        int getAvailableProcessors();

    }

    public static interface HostUsageStatistics {

        double getSystemCpuLoad();

        double getJvmCpuLoad();

        long getHostMemoryUsage();

        long getHeapMemoryUsage();

        long getHeapMemoryTotal();
        
        long getNonHeapMemoryUsage();
        
        long getNonHeapMemoryTotal();

        int calculateDynamicPoolsize();
    }

    public static interface ResourceStatistics {

        default ComponentProgress getComponentProgress() {
            return (ComponentProgress) this;
        }

        default HostConfig getHostConfig() {
            return (HostConfig) this;
        }

        default HostUsageStatistics getHostUsageStatistics() {
            return (HostUsageStatistics) this;
        }

        Map<String, Object> getResourceStatistics();
    }

    public static interface ComponentProgress {

        int getCompletedInstances(String uuid);
        
        int getTotalInstances(String uuid);
        
        boolean isComponentCompleted(String uuid);

        int getComponentPipelineLevel(String uuid);
        
        int getCurrentPipelineLevel();

        int getRegisteredDocumentCount();

        Map<String, Integer> getComponentLevels(); 
    }

    class SystemResourceStatistics implements ResourceStatistics, ComponentProgress, HostConfig, HostUsageStatistics {

        final OperatingSystemMXBean _os = ManagementFactory.getOperatingSystemMXBean();
        final ThreadMXBean _threads = ManagementFactory.getThreadMXBean();
        final MemoryMXBean _memory = ManagementFactory.getMemoryMXBean();
        final Runtime _runtime = Runtime.getRuntime();
        final Map<String, Object> _static = new ConcurrentHashMap<>(6);
        final Map<String, Object> _dynamic = new ConcurrentHashMap<>(16);
        final Map<String, Map<String, Object>> _threadStats = new ConcurrentHashMap<>();

        final Set<IDUUIResource> _skipCollection = new HashSet<>(16);
        final Map<String, Object> _resourceStats = new ConcurrentHashMap<>();
        // Format:  
        // Resource name (e.g. DUUIDockerDriver) => ResourceStats (e.g list of container stats in maps) 

        Map<String, AtomicInteger> _completedComponentInstances = new ConcurrentHashMap<>(1);
        Map<String, Integer> _heightMap = new ConcurrentHashMap<>(1);
        AtomicInteger _currentLevel = new AtomicInteger(0);
        AtomicInteger _registeredDocumentsCount = new AtomicInteger(0);
        String _currentInstance = "";

        final long _thresholdBytes;
        long _usedBytes = 0L; 
        AtomicBoolean _poolSizeSet = new AtomicBoolean(false);

        {
            // try {
            //     _memory.setVerbose(true);
            // } catch (Exception e) {
            // }
            try {
                _threads.setThreadContentionMonitoringEnabled(true);
                _threads.setThreadCpuTimeEnabled(true);
            } catch (Exception e) {
                System.out.println("[DUUIResourceManager] Thread monitoring limited.");

            }
        }
        
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

        @Override
        public Map<String, Object> getResourceStatistics() {
            return _resourceStats;
        }

        // HOST CONFIG

        public long getMemoryThreshhold() {
            return _thresholdBytes;
        }

        public long getJVMMaxMemory() {
            return (long) _static.get("os_jvm_max_memory");
        }

        public long getHostMemoryTotal() {
            return (long) _dynamic.get("system_memory_total");
        }

        public int getAvailableProcessors() {
            return (int) _static.get("os_processors");
        }

        // HOST USAGE STATISTICS

        public double getSystemCpuLoad() {
            return (double) _dynamic.get("cpu_system_load");
        }

        public double getJvmCpuLoad() {
            return (double) _dynamic.get("cpu_jvm_load");
        }

        public long getHostMemoryUsage() {
            return (long) _dynamic.get("system_memory_used");
        }

        public long getHeapMemoryTotal() {
            return (long) _dynamic.get("memory_heap_committed");
        }

        public long getHeapMemoryUsage() {
            return (long) _dynamic.get("memory_heap_used");
        }

        public long getNonHeapMemoryTotal() {
            return (long) _dynamic.get("memory_non_heap_committed");
        }

        public long getNonHeapMemoryUsage() {
            return (long) _dynamic.get("memory_non_heap_used");
        }

        public int calculateDynamicPoolsize() {
            long[] cumulatedCpuTime = {0L};
            long[] cumulatedWaitTime = {0L};

            // _threadStats.keySet().stream()
            // .map(Long::valueOf)
            // .filter(_workerThreads::containsKey)
            // .map(String::valueOf)
            // .forEach(threadId -> {
            //     cumulatedWaitTime[0] += (long) _threadStats
            //         .get(threadId).get("thread_total_wait_time");
            //     cumulatedCpuTime[0] += (long) _threadStats
            //         .get(threadId).get("thread_cpu_time");
            // });


            double blockingFactor;
            // System.out.println("CUMULATED CPU TIME: " + cumulatedCpuTime[0]);
            // System.out.println("CUMULATED WAIT TIME: " + cumulatedWaitTime[0]);
            if (cumulatedCpuTime[0] == 0L || cumulatedWaitTime[0] == 0L)
                blockingFactor = 0.5;
            else
                blockingFactor = cumulatedCpuTime[0] / cumulatedWaitTime[0];

            return (int) Math.round(getAvailableProcessors() / (1 - blockingFactor));
        }

        // COMPONENT PROGRESS
    
        @Override
        public Map<String, Integer> getComponentLevels() {
            return _heightMap;
        }

        @Override
        public ComponentProgress getComponentProgress() {
            return this;
        }

        @Override
        public int getRegisteredDocumentCount() {
            return _registeredDocumentsCount.get();
        }

        @Override
        public int getComponentPipelineLevel(String uuid) {
            return _heightMap.get(uuid);
        }

        @Override 
        public boolean isComponentCompleted(String uuid) {
            return getTotalInstances(uuid) == getCompletedInstances(uuid);
        }

        @Override
        public int getTotalInstances(String uuid) {
            return _registeredDocumentsCount.get();
        }

        @Override
        public int getCompletedInstances(String uuid) {
            AtomicInteger completed = _completedComponentInstances.get(uuid);

            if (completed == null) return -1;

            return completed.get();
        }

        @Override
        public int getCurrentPipelineLevel() {
            return _currentLevel.get();
        }

        // COLLECTION METHODS

        Map<String, Object> collect() {
            collectHostData();
            collectMemoryData();

            return _dynamic; 
        }

        Map<String, Object> collectStaticData() {

            _static.put("os_processors", _os.getAvailableProcessors());
            _static.put("os_version", _os.getVersion());
            _static.put("os_name", _os.getName());
            _static.put("os_arch", _os.getArch());
            _static.put("os_jvm_vendor", ManagementFactory.getRuntimeMXBean().getVmVendor());
            _static.put("os_jvm_max_memory", _runtime.maxMemory());

            return _static;
        }

        Map<String, Object> collectResourcesData() {
            for (IDUUIResource resource : _resources) {
                if (_skipCollection.contains(resource)) 
                    continue;
                    
                try {
                    // size += serialize(resource);
                    Map<String, Object> resourceStats = resource.collect();
                    if (resourceStats == null) continue; 

                    String clazz = resource.getClass().getSimpleName();

                    if (resource instanceof DUUIParallelPipelineExecutor) {
                        _currentLevel = (AtomicInteger) 
                            resourceStats.get("currentLevel");
                        _registeredDocumentsCount = (AtomicInteger)
                            resourceStats.get("registeredDocumentsCount");
                        _completedComponentInstances = (Map<String, AtomicInteger>)
                            resourceStats.get("completedComponentInstances");
                        _heightMap = (Map<String, Integer>)
                             resourceStats.get("heightMap");

                        _skipCollection.add(resource);
                    } else {
                        _resourceStats.put(clazz, resourceStats.get(clazz));
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "[DUUIResourceManager] Error collecting resource stats: %s%n%s%n",
                        e, e.getMessage());
                }
            }

            return _resourceStats; 
        }

        void collectHostData() {
            double cpuLoad = -1; 
            double systemLoad = -1;
            long jvm_cpu_time = -1; 
            long systemTotalMemory = -1;
            long systemUsedMemory = -1; 

            if (_os instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean osSun = (com.sun.management.OperatingSystemMXBean) _os;
                cpuLoad = osSun.getProcessCpuLoad();  // -1 => not available
                systemLoad = osSun.getSystemCpuLoad(); // -1 => not available
                jvm_cpu_time = osSun.getProcessCpuTime();
                systemTotalMemory = osSun.getTotalPhysicalMemorySize();
                systemUsedMemory = osSun.getTotalPhysicalMemorySize() - osSun.getFreePhysicalMemorySize();
            }

            _dynamic.put("cpu_load_average", _os.getSystemLoadAverage());
            _dynamic.put("cpu_jvm_load", cpuLoad);
            _dynamic.put("cpu_system_load", systemLoad);
            _dynamic.put("system_memory_total", systemTotalMemory);
            _dynamic.put("system_memory_used", systemUsedMemory);
            _dynamic.put("thread_stats", collectThreadData(jvm_cpu_time));
        }

        void collectMemoryData() {
            _dynamic.put("memory_heap_used", _runtime.totalMemory() - _runtime.freeMemory());
            _dynamic.put("memory_heap_committed", _runtime.totalMemory());

            MemoryUsage stackUsage = _memory.getNonHeapMemoryUsage();
            _dynamic.put("memory_non_heap_used", stackUsage.getUsed());
            _dynamic.put("memory_non_heap_committed", stackUsage.getCommitted());
            _dynamic.put("memory_used", _runtime.totalMemory() - _runtime.freeMemory());
            _dynamic.put("memory_total", _runtime.totalMemory());
            
            stackUsage = null;
        }

        List<Map<String, Object>> collectThreadData(long jvm_cpu_time) {
            List<Map<String, Object>> threadStats = new ArrayList<>(_activeThreads.size());

            for (Thread th : _activeThreads) {
                long id = th.getId();
                try {
                    Map<String, Object> threadStat; 
                    if (_threadStats.containsKey(String.valueOf(id))) {
                        threadStat = _threadStats.get(String.valueOf(id));
                    } else {
                        threadStat = new HashMap<>(10);
                        _threadStats.put(String.valueOf(id), threadStat);
                    }
    
                    ThreadInfo info = _threads.getThreadInfo(id);
                    if (info == null) continue;

                    long memoryUsage = -1L;
                    if (_threads instanceof com.sun.management.ThreadMXBean) {
                        com.sun.management.ThreadMXBean threadsSun = 
                            (com.sun.management.ThreadMXBean) _threads;
                        memoryUsage = threadsSun.getThreadAllocatedBytes(id);
                    }
    
                    threadStat.put("thread_id", id);
                    threadStat.put("thread_name", th.getName());
                    threadStat.put("thread_state", info.getThreadState().toString());
                    threadStat.put("thread_total_wait_time", info.getWaitedTime()); // milliseconds
                    threadStat.put("thread_total_block_time", info.getBlockedTime());
                    threadStat.put("thread_cpu_time", _threads.getThreadCpuTime(id)); // nanoseconds
                    threadStat.put("jvm_cpu_time", jvm_cpu_time); // nanoseconds
                    threadStat.put("thread_memory_usage", memoryUsage);
    
                    threadStats.add(threadStat);
                } catch (Exception e) {
                    System.out.println(
                        "[DUUIResourceManager] Exception getting thread statistics. Thread: " +
                        th.getName());
                    System.out.printf("%s: %s%n", e, e.getMessage());
                }
            }

            return threadStats;
        }

    }
}
