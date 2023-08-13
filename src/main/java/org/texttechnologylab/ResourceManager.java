package org.texttechnologylab;

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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUISimpleMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.IDUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.PoolStrategy;

public class ResourceManager extends Thread {

    final List<IDUUIResource> _resources = new ArrayList<>(); 
    final AtomicBoolean _finished = new AtomicBoolean(false);
    static final HashSet<Thread> _activeThreads = new HashSet<>(20);

    CasPool _casPool;     

    final SystemResources _system; 
    boolean memoryCritical = false;
    final ReentrantLock memoryLock = new ReentrantLock();
    final Condition unpaused = memoryLock.newCondition();

    static final ConcurrentLinkedQueue<Runnable> _tasks = new ConcurrentLinkedQueue<>(); 
    public DUUISimpleMonitor _monitor = null; 
    static final ResourceManager _rm = new ResourceManager();
    

    ResourceManager() {
        super("DUUIResourceManager");
        // this.setDaemon(true);
        _activeThreads.add(this);
        
        _system = new SystemResources(0.01, -1L);
    }

    public static ResourceManager getInstance() {
        return _rm; 
    };

    public void withMonitor(IDUUIMonitor monitor) {
        _monitor = (DUUISimpleMonitor) monitor;
    }
   
    public synchronized static void register(Thread thread) {
        _activeThreads.add(thread);
    }

    public void register(IDUUIResource resource) {
        _resources.add(resource);
    }

    @Override
    public void run() {
        try {
            dispatch(_system.collectStaticData(), 
                DUUISimpleMonitor.V1_MONITOR_SYSTEM_STATIC_INFO);

            while (! _finished.get()) {
                try {
                    Thread.sleep(500);
                    
                    resumeWhenMemoryFree();
            
                    dispatchSystemDynamicInfo();
                } catch (IOException e) {
                    System.out.printf(
                        "[DUUIResourceManager] Exception occurred: %s%n", e);
                    e.printStackTrace();
                }
            }

            dispatchResourceInfo();
        } catch (IOException e) {
            System.out.printf("[DUUIResourceManager] Exception occurred: %s%n", e);
            e.printStackTrace();
        } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
        }
    }

    public void finishManager() {
        _finished.set(true);
    }
    
    public void waitIfMemoryCritical() {
        if (_system.isMemoryCritical()) {
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
        }

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
        if (_system.isMemoryCritical()) 
            return;

        long used = _system._usedBytes;
        long thresholdBytes = _system._thresholdBytes;
        memoryLock.lock();
        try {
            if ( used <  thresholdBytes*0.8) {
                // System.out.printf(
                //     "MEMORY SAFE! Used: %d | Threshhold: %d %n", used, thresholdBytes);
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
        waitIfMemoryCritical();
        return _casPool.takeCas();
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

    // TODO: Cleanup 
    private void dispatchResourceInfo() throws IOException {

        JSONObject resources = new JSONObject();
        try {
            for (IDUUIResource resource : _resources) {
                JSONObject resourceStats = resource.collect();
                if (resourceStats != null) 
                    resources.put(resourceStats.getString("key"), 
                        resourceStats.get(resourceStats.getString("key")));
            }
        } catch (Exception e) {
            System.out.println("[DUUIResourceManager] Error collecting resource stats.");
            e.printStackTrace();
        }

        try {
            _monitor.sendUpdate(resources, DUUISimpleMonitor.V1_MONITOR_RESOURCE_INFO);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void dispatchSystemDynamicInfo() throws IOException {
        dispatch(_system.collect(), DUUISimpleMonitor.V1_MONITOR_SYSTEM_DYNAMIC_INFO);
    }

    void dispatch(Map<String, Object> resourceData, String type) {    
        if (_monitor == null)
            return;  
        try {
            _monitor.sendUpdate(resourceData, type);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
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


        CasPool(PoolStrategy strategy, TypeSystemDescription desc) throws UIMAException {
            _maxPoolSize = new AtomicInteger(strategy.getMaxPoolSize());
            _strategy = strategy;
            _casPool = _strategy.instantiate(JCas.class);
            _bytestreams = _strategy.instantiate(ByteArrayOutputStream.class);
            _casMonitorPool = new ArrayList<>(_strategy.getCorePoolSize());
            
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
            
        }

        void setMaxPoolSize() {
            _maxPoolSize.set(_casMonitorPool.size()); 
        }
        
        void returnCas(JCas jc) {
            System.out.printf("JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size());
            _casPool.add(jc);

        }

        void returnByteStream(ByteArrayOutputStream bs) {
            System.out.printf("BYTESTREAM QUEUE CAPACITY: %d | #BYTESTREAM: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currStreamPoolSize.get(), _bytestreams.size());
            _bytestreams.add(bs);
        }

        JCas takeCas () throws InterruptedException {
                System.out.printf("JCAS QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currCasPoolSize.get(), _casPool.size());
            return take(_casPool, _casSupplier, _currCasPoolSize);
        }

        ByteArrayOutputStream takeStream() throws InterruptedException {
            System.out.printf("BYTESTREAM QUEUE CAPACITY: %d | #BYTESTREAM: %d | #RESERVED: %d %n", _maxPoolSize.get(), _currStreamPoolSize.get(), _bytestreams.size());
            return take(_bytestreams, _streamSupplier, _currStreamPoolSize);
        }

        <T> T take(BlockingQueue<T> pool, Supplier<T> generator, AtomicInteger poolSize) throws InterruptedException {
            T resource = pool.poll(_strategy.getTimeOut(TimeUnit.SECONDS), TimeUnit.SECONDS);

            if (resource != null)
                return resource; 

            if (poolSize.get() < _maxPoolSize.get()) {
                T t = generator.get();
                pool.add(t);
                poolSize.incrementAndGet();
                if (t instanceof JCas) {
                    JCas jc = (JCas) t;
                    _casMonitorPool.add(jc);
                    System.out.printf("JCAS NEW ELEMENT ADDED: QUEUE CAPACITY: %d | #JCAS: %d | #RESERVED: %d %n", _maxPoolSize.get(), poolSize.get(), _casPool.size());
                } else {
                    System.out.printf("BYTESTREAM NEW ELEMENT ADDED: QUEUE CAPACITY: %d | #BYTESTREAM: %d | #RESERVED: %d %n", _maxPoolSize.get(), poolSize.get(), _bytestreams.size());
                }
            }

            return pool.take(); 
        }

        long getJCasMemoryConsumption() {
            return _casMonitorPool.stream()
                .mapToLong(JCas::size)
                .sum();
        }

    }

    class SystemResources {

        final OperatingSystemMXBean _os = ManagementFactory.getOperatingSystemMXBean();
        final ThreadMXBean _threads = ManagementFactory.getThreadMXBean();
        final MemoryMXBean _memory = ManagementFactory.getMemoryMXBean();
        final Runtime _runtime = Runtime.getRuntime();
        final Map<String, Object> _static = new HashMap<>(6);
        final Map<String, Object> _dynamic = new HashMap<>(16);
        
        double _memoryThreshhold = 0.1;
        final long _thresholdBytes;
        long _usedBytes = 0L; 

        {
            // try {
            //     _memory.setVerbose(true);
            // } catch (Exception e) {
            //     System.out.println("[DUUIResourceManager] Memory monitoring limited.");
            // }
            try {
                _threads.setThreadContentionMonitoringEnabled(true);
                _threads.setThreadCpuTimeEnabled(true);
            } catch (Exception e) {
                System.out.println("[DUUIResourceManager] Thread monitoring limited.");

            }
        }
        
        public SystemResources(double memoryThreshhold, long thresholdBytes) {
            if (memoryThreshhold == -1 && thresholdBytes == -1) 
                _thresholdBytes = Math.round(_memoryThreshhold *_runtime.maxMemory());
            else if (memoryThreshhold != -1)
                _thresholdBytes = Math.round(memoryThreshhold *_runtime.maxMemory());
            else 
                _thresholdBytes = thresholdBytes;
        }

        boolean isMemoryCritical() {
            if (_casPool == null) 
                return false; 
            _usedBytes = _casPool.getJCasMemoryConsumption();
            boolean critical = _usedBytes > _thresholdBytes;
            if (critical) _casPool.setMaxPoolSize();
            return critical;
        }

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

            collectThreadData(jvm_cpu_time);
        }

        void collectMemoryData() {
            MemoryUsage heapUsage = _memory.getHeapMemoryUsage();
            _dynamic.put("memory_heap_used", heapUsage.getUsed());
            _dynamic.put("memory_heap_committed", heapUsage.getCommitted());

            MemoryUsage stackUsage = _memory.getNonHeapMemoryUsage();
            _dynamic.put("memory_non_heap_used", stackUsage.getUsed());
            _dynamic.put("memory_non_heap_committed", stackUsage.getCommitted());
            _dynamic.put("memory_used", _runtime.totalMemory() - _runtime.freeMemory());
            _dynamic.put("memory_total", _runtime.totalMemory());
        }

        void collectThreadData(long jvm_cpu_time) {

            long[] ids = _threads.getAllThreadIds();
            ThreadInfo[] infos = _threads.getThreadInfo(ids);
            long[] bytes = new long[1];

            if (_threads instanceof com.sun.management.ThreadMXBean) {
                com.sun.management.ThreadMXBean threadsSun = (com.sun.management.ThreadMXBean) _threads;
                
                bytes = threadsSun.getThreadAllocatedBytes(ids);
            }

            List<Map<String, Object>> array;
            if (_dynamic.containsKey("thread_stats")) {
                array = (List<Map<String, Object>>) _dynamic.get("thread_stats");
            } else {
                array = new ArrayList<>();
                _dynamic.put("thread_stats", array);
            }

            Set<Long> _duuiThreads = _activeThreads.stream().map(th -> th.getId()).collect(
                Collectors.toSet());

            for (int i = 0; i < ids.length; i++) {
                if (infos[i] == null ) continue;
                if (!_duuiThreads.contains(ids[i])) continue; 

                Map<String, Object> thread = new HashMap<>();
                thread.put("thread_id", ids[i]);
                thread.put("thread_name", infos[i].getThreadName());
                thread.put("thread_state", infos[i].getThreadState().toString());
                thread.put("thread_total_wait_time", infos[i].getWaitedTime()); // milliseconds
                thread.put("thread_total_block_time", infos[i].getBlockedTime());
                thread.put("thread_cpu_time", _threads.getThreadCpuTime(ids[i])); // nanoseconds
                thread.put("jvm_cpu_time", jvm_cpu_time); // nanoseconds
                long memoryUsage = -1L; 
                if (bytes.length == ids.length)
                    memoryUsage = bytes[i]; 
                thread.put("thread_memory_usage", memoryUsage);
                array.add(thread);
            }
        }
    }

    // static class ResettableLock {

    //     private CountDownLatch latch; 
        
    //     public ResettableLock() {
    //         latch = new CountDownLatch(1);
    //     }

    //     public void await() {
    //         try {
    //             latch.await();
    //         } catch (InterruptedException e) {
    //         }
    //     }

    //     public void release() {
    //         latch.countDown();
    //         synchronized(latch) {
    //             latch = new CountDownLatch(1);
    //         }
    //     }
    // }
}
