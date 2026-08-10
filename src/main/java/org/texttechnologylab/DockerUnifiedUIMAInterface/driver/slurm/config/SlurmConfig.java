package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SlurmConfig {
    @JsonProperty("container_mode")
    private ContainerMode containerMode;

    @JsonProperty("physical_mode")
    private PhysicalMode physicalMode;

    public ContainerMode getContainerMode() {
        return containerMode;
    }

    public void setContainerMode(ContainerMode containerMode) {
        this.containerMode = containerMode;
    }

    public PhysicalMode getPhysicalMode() {
        return physicalMode;
    }

    public void setPhysicalMode(PhysicalMode physicalMode) {
        this.physicalMode = physicalMode;
    }
}
