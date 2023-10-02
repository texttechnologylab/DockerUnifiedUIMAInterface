package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation;

import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatb;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatns;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatms;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatp;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.DockerDriverView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource.DockerContainerView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResourceProfiler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.HostConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.HostThreadView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.HostUsage;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.PipelineProgress;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;

public class DUUIConsoleMonitor implements IDUUIMonitor, IDUUIResourceProfiler {
    

    public DUUIConsoleMonitor() {
    }

    public DUUIConsoleMonitor setup() {
        return this;
    }
    
    public void addMeasurements(ResourceViews views, boolean pipelineStarted) {

        final HostConfig config = views.getHostConfig();

        final PipelineProgress progress = views.getComponentProgress();
        String leftAlignFormat = "| %-18s | %-13s | %-10s | %-12s |%n";
        System.out.format("+--------------------+---------------+------------+--------------+%n");
        System.out.format("| Pipeline: Progress | Current Level | Next Level | Remaining |%n");
        System.out.format(leftAlignFormat, 
            progress.getLevelProgress(), progress.getCurrentLevel(), progress.getNextLevel(), formatns(progress.getRemainingNanos()));
        System.out.format("+--------------------+---------------+------------+--------------+%n");
        System.out.println();

        leftAlignFormat = "| %-12s | %-15s | %-8d | %-4d | %-4d | %-5s | %-7s |%n";
        System.out.format("+--------------+-----------------+----------+------+-------+---------+%n");
        System.out.format("|      OS      | Core / Max Pool | CAS-Pool | CPUs |  RAM  | JVM-RAM |%n");
        System.out.format("+--------------+-----------------+----------+------+-------+---------+%n");
        System.out.format(leftAlignFormat, config.getOSName(), Config.strategy().getCorePoolSize(), 
            Config.strategy().getMaxPoolSize(), config.getCASPoolSize(), config.getAvailableProcessors(), 
            formatb(config.getHostMemoryTotal()), formatb(config.getJVMMaxMemory())
        );
        System.out.format("+--------------+-----------------+----------+------+-------+---------+%n");
        
        System.out.println("SYSTEM RESOURCES");
        final HostUsage usage = views.getHostUsage();
        leftAlignFormat = "| %-4s / %-4s | %-5s | %-5s / %-5s |%n";
        System.out.format("+----------------+-----------+----------------+%n");
        System.out.format("| CPU: JVM / CPU | RAM Usage |  Heap / Total  |%n");
        System.out.format("+----------------+-----------+----------------+%n");
        System.out.format(leftAlignFormat, formatp(usage.getJvmCpuLoad()), formatp(usage.getSystemCpuLoad()), 
            formatb(usage.getHostMemoryUsage()), formatb(usage.getHeapMemoryUsage()), formatb(usage.getHeapMemoryTotal())
        );
        System.out.format("+----------------+-----------+----------------+%n");
        
        System.out.println("THREAD VIEWS");
        leftAlignFormat = "| %-60s | %-13s | %-6s | %-7s | %-7s | %-6s |%n";
        System.out.format("+--------------------------------------------------------------+---------------+--------+---------+---------+--------+%n");
        System.out.format("|                        Thread: Name                          |     State     | Waited | Running | Blocked | Memory |%n");
        System.out.format("+--------------------------------------------------------------+---------------+--------+---------+---------+--------+%n");
        for (HostThreadView threadview : usage.getThreadViews()) {
            System.out.format(leftAlignFormat, threadview.getName(), threadview.getState(), formatms(threadview.getWaitedTime()), 
                formatms(threadview.getCpuTime()), formatms(threadview.getBlockedTime()), 
                formatb(threadview.getMemoryUsage())
            );
            System.out.format("+--------------------------------------------------------------+---------------+--------+---------+---------+--------+%n");
        }

        // System.out.println("DOCKER CONTAINER VIEWS"); |
        // gerparcor_sample1000_smallest_adaptive10scale3-7-Token
        // leftAlignFormat = "| %-16s | %-5s | %-6s |%n";
        // System.out.format("+------------------+-------+--------+%n");
        // System.out.format("| Container: Image |  CPU  | Memory | %n");
        // System.out.format("+------------------+-------+--------+%n");
        // DockerDriverView driver = (DockerDriverView) views.getDockerDriverView();
        // for (DockerContainerView containerview : driver.getContainerViews()) {
        //     System.out.format(leftAlignFormat, containerview.getImage(), containerview.getCpuUsage(), formatb(containerview.getMemoryUsage()));
        //     System.out.format("+------------------+-------+--------+%n");
        // } Oracle Corporation
    }
}
