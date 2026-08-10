package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RestConfig {
    @JsonProperty("rest_available")
    private boolean available;

    @JsonProperty("rest_version")
    private String version;

    @JsonProperty("rest_port")
    private Integer restPort;

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getRestPort() {
        return restPort;
    }

    public void setRestPort(Integer restPort) {
        this.restPort = restPort;
    }
}
