package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.util.HashMap;
import java.util.Map;

import org.javatuples.Pair;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceViews;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceView;

import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;


public interface IDUUIResource<T extends ResourceView> {

    static HashMap<String, Pair<Float, Float>> preCPUStats = new HashMap<>();

    public default ResourceManager getResourceManager() {
        return ResourceManager.getInstance();
    }

    public default T collect() {
        return null; 
    }

    public default void scale(ResourceViews statistics) {
        // System.out.printf("[%s] Scaling unimplemented. %n", this.getClass().getSimpleName());

    }

    static class DockerContainerView implements ResourceView {

        public final String container_id;
        public final String image_id;
        String state;
        long memory_limit = -1l;
        long memory_usage = -1l;
        long memory_max_usage = -1l;
        long network_in = -1l;
        long network_out = -1l;
        double cpu_usage = -1.f; // percent [0.0, 100.0]

        double pre_cpu = -1.f; 
        double pre_system_cpu = -1.f;

        public DockerContainerView(String container_id, String image_id) {
            this.container_id = container_id;
            this.image_id = image_id;
        }

        public DockerContainerView stats(final DUUIDockerInterface docker) {
            try {   
                ContainerState state = docker.getDockerClient().inspectContainerCmd(container_id).exec().getState();
                final String stateStr = state.getStatus();
                
                final Statistics stats = docker.get_stats(container_id);
                
                long network_i = -1L;
                long network_o = -1L;
                if (! stats.getNetworks().isEmpty()) {
                    StatisticNetworksConfig network = stats.getNetworks().values().iterator().next();
                    network_i = network.getRxBytes();
                    network_o = network.getTxBytes();
                }

                final long memory_limit = stats.getMemoryStats().getLimit();
                final long memory_usage = stats.getMemoryStats().getUsage();
                final long memory_max_usage = stats.getMemoryStats().getMaxUsage();
                final long network_in = network_i;
                final long network_out = network_o;
                final double currSystemCPU = stats.getCpuStats().getSystemCpuUsage().doubleValue();
                final double currCPU = stats.getCpuStats().getCpuUsage().getTotalUsage().doubleValue();
                final int onlineCPUs = stats.getCpuStats().getOnlineCpus().intValue();

                update(stateStr, memory_limit, memory_usage, memory_max_usage, network_in, network_out, currSystemCPU, currCPU, onlineCPUs);
            } catch (Exception e) {
                System.out.printf("[%s] Error retrieving docker container stats: %s %n %s %n", 
                    Thread.currentThread().getName(), e,  e.getLocalizedMessage()    
                );
            }
            return this;  
        };

        void update(String state, long memory_limit, long memory_usage, long memory_max_usage, long network_in, long network_out, double curr_cpu, double curr_system_cpu, int cpus) {
            this.state = state;
            if (memory_limit > -1l) this.memory_limit = memory_limit;
            if (memory_limit > -1l) this.memory_usage = memory_usage;
            if (memory_limit > -1l) this.memory_max_usage = memory_max_usage;
            if (memory_limit > -1l) this.network_in = network_in;
            if (memory_limit > -1l) this.network_out = network_out;

            if (pre_cpu > -1.f) cpu_usage = computeCpuLoad(curr_cpu, curr_system_cpu, pre_cpu, pre_system_cpu, cpus);
            pre_cpu = curr_cpu;
            pre_system_cpu = curr_system_cpu;
        }

        /*
        * 
        * Taken from: https://shihtiy.com/posts/ECS-calculate-CPU-utilization-metadata-endpoing/
        */
        static double computeCpuLoad(double curr_cpu, double curr_system_cpu, double pre_cpu, double pre_system_cpu, int cpus) {
            final double cpuDelta = curr_cpu - pre_cpu; 
            final double systemDelta = curr_system_cpu - pre_system_cpu; 
            final double cpuLoad = (cpuDelta / systemDelta) * cpus * 100f; 
            return cpuLoad;
        }
    }

    // public static Map<String, Object> getContainerStats(
    //     DUUIDockerInterface _interface, 
    //     Map<String, Object> containerStats,  
    //     String containerId, 
    //     String imageId) {

    //     try {   
    //         ContainerState state = _interface.getDockerClient().inspectContainerCmd(containerId).exec().getState();
            
    //         String status = state.getStatus();
    //         containerStats.put("status", status);

    //         Statistics stats = _interface.get_stats(containerId);
    //         float cpuPercent = getCPUStats(containerId, stats);
            
    //         long network_i = -1L;
    //         long network_o = -1L;
    //         if (! stats.getNetworks().isEmpty()) {
    //             StatisticNetworksConfig network = stats.getNetworks().values().iterator().next();
    //             network_i = network.getRxBytes();
    //             network_o = network.getTxBytes();
    //         }

    //         containerStats.put("memory_limit", stats.getMemoryStats().getLimit());
    //         containerStats.put("memory_usage", stats.getMemoryStats().getUsage());
    //         containerStats.put("memory_max_usage", stats.getMemoryStats().getMaxUsage());
    //         containerStats.put("num_procs", stats.getNumProcs());
    //         containerStats.put("network_i", network_i);
    //         containerStats.put("network_o", network_o);
    //         containerStats.put("cpu_usage", cpuPercent);
    //         containerStats.put("container_id", containerId);
    //         containerStats.put("image_id", imageId);
    //         return containerStats; 
    //     } catch (Exception e) {
    //         System.out.printf("[%s] Error retrieving docker container stats: %s %n %s %n", 
    //             Thread.currentThread().getName(), e,  e.getLocalizedMessage()    
    //         );
    //         return containerStats;  
    //     }
    // };

}
