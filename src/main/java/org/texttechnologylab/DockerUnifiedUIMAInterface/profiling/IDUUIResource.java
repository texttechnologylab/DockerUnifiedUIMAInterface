package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;

import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;


/**
 * Interface for the {@link ResourceManager} to interact with various parts of DUUI. 
 * {@link IDUUIResources} send {@link ResourceView}s to the {@link ResourceManager}. 
 * 
 */
public interface IDUUIResource<T extends ResourceView> {

    public default ResourceManager getResourceManager() {
        return ResourceManager.getInstance();
    }

    /**
     * Method invoked by the {@link ResourceManager} to {@link ResourceView}'s from {@link IDUUIResource}s. 
     * 
     */
    public default T collect() {
        return null; 
    }

    /**
     * Method invoked by the {@link ResourceManager} to scale Components. 
     * 
     * @param statistics Information related to the system, the pipeline and other Components.
     */
    public default void scale(ResourceViews statistics) {
        // System.out.printf("[%s] Scaling unimplemented. %n", this.getClass().getSimpleName());

    }

    /**
     * {@link ResourceView} for Docker Containers. 
     * Can be used by {@link DUUIDockerDriver} or {@link DUUIDockerSwarmDriver}.
     * 
     * Currently the {@link DockerContainerView#stats(DUUIDockerInterface)} is slow limiting it's use cases.
     * 
     * 
     */
    public static class DockerContainerView implements ResourceView {

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

        /**
         * @return Container's ID.
         */
        public String getContainerId() {
            return container_id;
        }

        /**
         * @return Name of the Container's image.
         */
        public String getImage() {
            return image_id;
        }

        /**
         * @return Current state of the Container. {created, running, exited, paused}
         */
        public String getState() {
            return state;
        }

        /**
         * @return Most recent CPU usage of the Container as a number between 0.0 and 100.0.
         */
        public double getCpuUsage() {
            return cpu_usage;
        }

        /**
         * @return Peak memory usage among all measurements.
         */
        public long getPeakMemoryUsage() {
            return memory_max_usage;
        }

        /**
         * @return Most recent memory usage of the Container in bytes.
         */
        public long getMemoryUsage() {
            return memory_usage;
        }

        /**
         * @return Bytes sent to the Container through the network.
         */
        public long getNetworkIn() {
            return network_in;
        }

        /**
         * @return Bytes sent by the Container through the network.
         */
        public long getNetworkOut() {
            return network_out;
        }

        /**
         * 
         * @param docker Object used to connect to the Docker Engine.
         *
         * @return View containing statistics about a Container.
         */
        public DockerContainerView stats(final DUUIDockerInterface docker) {
            try {   
                final long start = System.currentTimeMillis();
                ContainerState state = docker.getDockerClient().inspectContainerCmd(container_id).exec().getState();
                final String stateStr = state.getStatus();
                System.out.println("DOCKER STATS COLLECTION TIME: " + (System.currentTimeMillis() - start));
                
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

        /**
         * Method to set the fields of this View. 
         * 
         * @param state Current state of a Docker Container. [created, running, exited, paused.]
         * @param memory_limit Memory threshold for a Container. [bytes]
         * @param memory_usage Current memory usage. [bytes]
         * @param memory_max_usage Peak memory usage among all measurements. [bytes]
         * @param network_in Amount of bytes sent in through the network. [bytes]
         * @param network_out Amount of bytes sent by the Container through the network. [bytes]
         * @param curr_cpu Current CPU-usage of the Container. [ns]
         * @param curr_system_cpu Current CPU-usage of the entire system. [ns]
         * @param cpus Number of CPUs in the machine running the Container.
         */
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

        /**
        * Taken from: https://shihtiy.com/posts/ECS-calculate-CPU-utilization-metadata-endpoing/
        * Function used to compute a Container's CPU-utilization.
        *
        * @param curr_cpu CPU-usage of the Container from the current measurement.
        * @param curr_system_cpu CPU-usage of the system from the current measurement.
        * @param pre_cpu CPU-usage of the Container from the previous measurement.
        * @param pre_system_cpu CPU-usage of the system from the previous measurement.
        * @param cpus Number of CPU's in the machine. 
        *
        */
        static double computeCpuLoad(double curr_cpu, double curr_system_cpu, double pre_cpu, double pre_system_cpu, int cpus) {
            final double cpuDelta = curr_cpu - pre_cpu; 
            final double systemDelta = curr_system_cpu - pre_system_cpu; 
            final double cpuLoad = (cpuDelta / systemDelta) * cpus * 100f; 
            return cpuLoad;
        }
    }

}
