package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.time.Instant;
import java.util.HashMap;

import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.ResourceManager;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;


public interface IDUUIResource {

    static HashMap<String, Pair<Float, Float>> preCPUStats = new HashMap<>();

    public ResourceManager getResourceManager();

    public void setResourceManager(ResourceManager rm);

    public default JSONObject collect() {
        return null; 
    }

    public default JSONObject getContainerStats(DUUIDockerInterface _interface, JSONObject containerStats,  String containerId, String imageId) {
        try {
            ContainerState state = _interface.getDockerClient().inspectContainerCmd(containerId).exec().getState();
            String status = state.getStatus();
            containerStats.put("status", status);
        } catch (Exception e) {
            System.out.println(e);
            return containerStats; 
        }
        try {
            Statistics stats = _interface.get_stats(containerId);

            float preCPU; float preSystemCpu;
            if (preCPUStats.containsKey(containerId)) {
                preCPU = preCPUStats.get(containerId).getValue0();
                preSystemCpu = preCPUStats.get(containerId).getValue1();
            } else {
                preCPU = stats.getPreCpuStats().getCpuUsage().getTotalUsage().floatValue();
                preSystemCpu = stats.getPreCpuStats().getSystemCpuUsage().floatValue();
                preCPUStats.put(containerId, Pair.with(preCPU, preSystemCpu));
            }

            float cpuPercent = -1.0f;
            float cpuDelta = stats.getCpuStats().getCpuUsage().getTotalUsage().floatValue() - preCPU;
            float systemDelta = stats.getCpuStats().getSystemCpuUsage().floatValue() - preSystemCpu;
            cpuPercent = (cpuDelta / systemDelta) * stats.getCpuStats().getOnlineCpus() * 100F;
            
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
            System.out.println(e);
            return containerStats;  
        }
    }
}
