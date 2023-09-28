package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling;

import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;

public interface IDUUIResourceProfiler {
    
    void addMeasurements(ResourceViews views, boolean pipelineStarted);

    default void addMeasurements(ResourceViews views) {
        addMeasurements(views, false);
    };
}
