package org.texttechnologylab;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUISimpleMonitor;

public class ResourceManager extends Thread implements Executor{
    
    ArrayBlockingQueue<ByteArrayOutputStream> _bytestreams;
    final List<IDUUIResource> _resources = new ArrayList<>(); 
    final OperatingSystemMXBean _os = ManagementFactory.getOperatingSystemMXBean();
    final ThreadMXBean _threads = ManagementFactory.getThreadMXBean();
    final MemoryMXBean _memory = ManagementFactory.getMemoryMXBean();
    final Runtime _runtime = Runtime.getRuntime(); 
    final JSONObject _systemDynamicInfo = new JSONObject();
    final JSONObject _systemStaticInfo = new JSONObject();
    public final ResettableLock _lock = new ResettableLock();
    public final AtomicBoolean _withMonitor = new AtomicBoolean(false);
    final AtomicBoolean _finished = new AtomicBoolean(false);
    static final HashSet<Thread> _activeThreads = new HashSet<>(20);

    static final ConcurrentLinkedQueue<Runnable> _tasks = new ConcurrentLinkedQueue<>(); 
    public DUUISimpleMonitor _monitor = null; 
    boolean staticInfoSent = false; 
    static final ResourceManager _rm = new ResourceManager(); 
    

    private ResourceManager() {
        super("DUUIResourceManager");
        _activeThreads.add(this);
        // this.setDaemon(true);
        try {
            _threads.setThreadContentionMonitoringEnabled(true);
        } catch (Exception e) {
            System.out.println("[DUUIResourceManager] ThreadContentionMonitoring not available in current JVM." +
            "This means that thread-(blocked/waited)-time cannot be measured.");

        }

        _systemStaticInfo.put("os_processors", _os.getAvailableProcessors());
        _systemStaticInfo.put("os_version", _os.getVersion());
        _systemStaticInfo.put("os_name", _os.getName());
        _systemStaticInfo.put("os_arch", _os.getArch());
        _systemStaticInfo.put("os_jvm_vendor", ManagementFactory.getRuntimeMXBean().getVmVendor());
        _systemStaticInfo.put("os_jvm_max_memory", _runtime.maxMemory());

        this.start();
    }

    public synchronized static void register(Thread thread) {
        _activeThreads.add(thread);
    }

    public static ResourceManager getInstance() {
        return _rm; 
    };

    public void release() {
        _lock.release();
    }

    public void lock() {
        _lock.lock();
    }
    
    public void register(IDUUIResource resource) {
        _resources.add(resource);
    }

    
    @Override
    public void execute(Runnable command) {
        _tasks.add(command);
    }

    @Override
    public void run() {
        _lock.await();
        if (_withMonitor.get())
            _monitor = (DUUISimpleMonitor) Config.monitor();
        try {
            _monitor.sendUpdate(_systemStaticInfo, DUUISimpleMonitor.V1_MONITOR_SYSTEM_STATIC_INFO);
        } catch (IOException | InterruptedException e) {
            System.out.println(e);
        }
        _lock.lock();
        while (! _finished.get()) {
            try {
                dispatchSystemDynamicInfo();
                Thread.sleep(10);

                
            } catch (IOException | InterruptedException e) {
                System.out.printf("[DUUIResourceManager] Exception occurred: %s%n", e);
            }
        }

        try {
            dispatchResourceInfo();
        } catch (IOException e) {
            System.out.printf("[DUUIResourceManager] Exception occurred: %s%n", e);
        }
        _lock.release();
    }

    public void finished() {
        _finished.set(true);
        _lock.await();
    }

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
        
        // System.out.println(resources);

        // if (Config.monitor() != null)
        //     return; 
            
        if (_monitor == null)
            _monitor = (DUUISimpleMonitor) new DUUISimpleMonitor(); //Config.monitor();

        try {
            _monitor.sendUpdate(resources, DUUISimpleMonitor.V1_MONITOR_RESOURCE_INFO);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void dispatchSystemDynamicInfo() throws IOException {
        
        _systemDynamicInfo.put("cpu_load_average", _os.getSystemLoadAverage());
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
        _systemDynamicInfo.put("cpu_jvm_load", cpuLoad);
        _systemDynamicInfo.put("cpu_system_load", systemLoad);
        _systemDynamicInfo.put("system_memory_total", systemTotalMemory);
        _systemDynamicInfo.put("system_memory_used", systemUsedMemory);

        
        long[] ids = _threads.getAllThreadIds();
        ThreadInfo[] infos = _threads.getThreadInfo(ids);
        long[] bytes = new long[1];

        if (_threads instanceof com.sun.management.ThreadMXBean) {
            com.sun.management.ThreadMXBean threadsSun = (com.sun.management.ThreadMXBean) _threads;
            bytes = threadsSun.getThreadAllocatedBytes(ids);
        }

        JSONArray array;
        if (_systemDynamicInfo.has("thread_stats")) {
            array = _systemDynamicInfo.getJSONArray("thread_stats");
        } else {
            array = new JSONArray();
            _systemDynamicInfo.put("thread_stats", array);
        }

        Set<Long> _duuiThreads = _activeThreads.stream().map(th -> th.getId()).collect(
            Collectors.toSet());

        for (int i = 0; i < ids.length; i++) {
            if (infos[i] == null ) continue;
            if (!_duuiThreads.contains(ids[i])) continue; 

            JSONObject thread = new JSONObject();
            thread.put("thread_id", ids[i]);
            thread.put("thread_name", infos[i].getThreadName());
            thread.put("thread_state", infos[i].getThreadState().toString()); // TODO: Customize
            thread.put("thread_total_wait_time", infos[i].getWaitedTime()); // milliseconds
            thread.put("thread_total_block_time", infos[i].getBlockedTime());
            thread.put("thread_cpu_time", _threads.getThreadCpuTime(ids[i])); // nanoseconds
            thread.put("jvm_cpu_time", jvm_cpu_time); // nanoseconds
            long memoryUsage = -1L; 
            if (bytes.length == ids.length)
                memoryUsage = bytes[i]; 
            thread.put("thread_memory_usage", memoryUsage);
            array.put(thread);
        }
        

        MemoryUsage heapUsage = _memory.getHeapMemoryUsage();
        _systemDynamicInfo.put("memory_heap_used", heapUsage.getUsed());
        _systemDynamicInfo.put("memory_heap_committed", heapUsage.getCommitted());
        MemoryUsage stackUsage = _memory.getNonHeapMemoryUsage();
        _systemDynamicInfo.put("memory_non_heap_used", stackUsage.getUsed());
        _systemDynamicInfo.put("memory_non_heap_committed", stackUsage.getCommitted());
        _systemDynamicInfo.put("memory_used", _runtime.totalMemory() - _runtime.freeMemory());
        _systemDynamicInfo.put("memory_total", _runtime.totalMemory());
        

        // if (Config.monitor() != null)
        //     return; 
            
        if (_monitor == null)
            _monitor = (DUUISimpleMonitor) new DUUISimpleMonitor(); //Config.monitor();
        
        
        try {
            _monitor.sendUpdate(_systemDynamicInfo, DUUISimpleMonitor.V1_MONITOR_SYSTEM_DYNAMIC_INFO);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ResourceManager setByteStreams(int workers) {
        _bytestreams = new ArrayBlockingQueue<>(workers);
        
        Stream.generate(() -> new ByteArrayOutputStream(1024*1024))
            .limit(workers)
            .forEach(_bytestreams::add);
        
        return this; 
    }

    public ByteArrayOutputStream takeByteStream() throws InterruptedException {
        return _bytestreams.take();
    }

    public void returnByteStream(ByteArrayOutputStream bs) {
        _bytestreams.add(bs);
    }

    public static class ResettableLock {

        private CountDownLatch latch; 
        
        public ResettableLock() {
            latch = new CountDownLatch(1);
        }

        public void await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
            }
        }

        public void release() {
            latch.countDown();
        }

        public void lock() {
            synchronized(latch) {
                latch = new CountDownLatch(1);
            }
        }
    }

}
