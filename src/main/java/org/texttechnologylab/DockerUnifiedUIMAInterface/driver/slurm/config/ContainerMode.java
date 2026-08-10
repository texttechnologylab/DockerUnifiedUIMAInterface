package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ContainerMode {
    @JsonProperty("restapi")
    private RestConfig restConfig;

    @JsonProperty("pre_url")
    private String preURL;

    @JsonProperty("grafana_port")
    private Integer grafanaPort;

    @JsonProperty("es_port")
    private Integer esPort;

    @JsonProperty("free_port_from")
    private Integer freePortFrom;

    @JsonProperty("free_port_to")
    private Integer freePortTo;

    public RestConfig getRestConfig() {
        return restConfig;
    }

    public void setRestConfig(RestConfig restConfig) {
        this.restConfig = restConfig;
    }

    public String getPreURL() {
        return preURL;
    }

    public void setPreURL(String preURL) {
        this.preURL = preURL;
    }

    public Integer getGrafanaPort() {
        return grafanaPort;
    }

    public void setGrafanaPort(Integer grafanaPort) {
        this.grafanaPort = grafanaPort;
    }

    public Integer getEsPort() {
        return esPort;
    }

    public void setEsPort(Integer esPort) {
        this.esPort = esPort;
    }

    public Integer getFreePortFrom() {
        return freePortFrom;
    }

    public void setFreePortFrom(Integer freePortFrom) {
        this.freePortFrom = freePortFrom;
    }

    public Integer getFreePortTo() {
        return freePortTo;
    }

    public void setFreePortTo(Integer freePortTo) {
        this.freePortTo = freePortTo;
    }
}
