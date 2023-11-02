package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling;

import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;

/**
 * Interface for monitors or storage-backends to receive statistics from the {@link ResourceManager}.
 * 
 */
public interface IDUUIResourceProfiler {
    

    /**
     * Called at every cycle of the {@link ResourceManager} with the new resource-statistics.
     * 
     * @param views Resource statistics.
     * @param pipelineStarted True if this is the first call, false otherwise.
     */
    void addMeasurements(ResourceViews views, boolean pipelineStarted);

    /**
     * Called at every cycle of the {@link ResourceManager} with the new resource-statistics.
     * 
     * @param views Resource statistics.
     */
    default void addMeasurements(ResourceViews views) {
        addMeasurements(views, false);
    };
}
