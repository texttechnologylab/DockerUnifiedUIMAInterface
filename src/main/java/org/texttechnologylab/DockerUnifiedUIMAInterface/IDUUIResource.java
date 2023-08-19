package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.util.HashMap;
import java.util.Map;

import org.javatuples.Pair;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceStatistics;

import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;


public interface IDUUIResource {

    static HashMap<String, Pair<Float, Float>> preCPUStats = new HashMap<>();

    public static Map<String, Object> getContainerStats(
        DUUIDockerInterface _interface, 
        Map<String, Object> containerStats,  
        String containerId, 
        String imageId) {

        try {   
            ContainerState state = _interface.getDockerClient().inspectContainerCmd(containerId).exec().getState();
            String status = state.getStatus();
            containerStats.put("status", status);

            Statistics stats = _interface.get_stats(containerId);

            float cpuPercent = getCPUStats(containerId, stats);
            
            long network_i = -1L;
            long network_o = -1L;
            if (! stats.getNetworks().isEmpty()) {
                StatisticNetworksConfig network = stats.getNetworks().values().iterator().next();
                network_i = network.getRxBytes();
                network_o = network.getTxBytes();
            }

            containerStats.put("memory_limit", stats.getMemoryStats().getLimit());
            containerStats.put("memory_usage", stats.getMemoryStats().getUsage());
            containerStats.put("memory_max_usage", stats.getMemoryStats().getMaxUsage());
            containerStats.put("num_procs", stats.getNumProcs());
            containerStats.put("network_i", network_i);
            containerStats.put("network_o", network_o);
            containerStats.put("cpu_usage", cpuPercent);
            containerStats.put("container_id", containerId);
            containerStats.put("image_id", imageId);
            return containerStats; 
        } catch (Exception e) {
            System.out.printf("[%s] Error retrieving docker container stats: %s %n %s %n", 
                Thread.currentThread().getName(), e,  e.getLocalizedMessage()    
            );
            return containerStats;  
        }
    };

    /*
     * 
     * Taken from: https://shihtiy.com/posts/ECS-calculate-CPU-utilization-metadata-endpoing/
     */
    static float getCPUStats(String containerId, Statistics stats) {
        
        long onlineCPUs = stats.getCpuStats().getOnlineCpus();

        float preCPU = 0; 
        float preSystemCPU = 0;
        if (preCPUStats.containsKey(containerId)) {
            preCPU = preCPUStats.get(containerId).getValue0();
            preSystemCPU = preCPUStats.get(containerId).getValue1();
        }

        float currCPU = stats.getCpuStats().getCpuUsage().getTotalUsage().floatValue();
        float currSystemCPU = stats.getCpuStats().getSystemCpuUsage().floatValue();
        float cpuDelta = currCPU - preCPU; 
        float systemDelta = currSystemCPU - preSystemCPU; 

        float cpuLoad = (cpuDelta / systemDelta) * onlineCPUs * 100f; 

        preCPUStats.put(containerId, Pair.with(currCPU, currSystemCPU));

        return cpuLoad; 
    }

    public default ResourceManager getResourceManager() {
        return ResourceManager.getInstance();
    }

    public default Map<String, Object> collect() {
        return null; 
    }

    public default void scale(ResourceStatistics statistics) {
        // System.out.printf("[%s] Scaling unimplemented. %n", this.getClass().getSimpleName());

    }
    
}
