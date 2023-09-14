package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceViews;

public interface IDUUIResourceProfiler {
    
    void addMeasurements(ResourceViews views, boolean pipelineStarted);

    default void addMeasurements(ResourceViews views) {
        addMeasurements(views, false);
    };
}
