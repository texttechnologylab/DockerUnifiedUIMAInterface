package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PhysicalMode {
        @JsonProperty("local_cluster")
        private Cluster localCluster;

        @JsonProperty("remote_cluster")
        private Cluster remoteCluster;

    public Cluster getLocalCluster() {
        return localCluster;
    }

    public void setLocalCluster(Cluster localCluster) {
        this.localCluster = localCluster;
    }

    public Cluster getRemoteCluster() {
        return remoteCluster;
    }

    public void setRemoteCluster(Cluster remoteCluster) {
        this.remoteCluster = remoteCluster;
    }

    public static class Cluster{
        @JsonProperty("pre_url")
        private String pre_url;

        @JsonProperty("free_port_from")
        private Integer free_port_from;

        @JsonProperty("free_port_to")
        private Integer free_port_to;

        @JsonProperty("restapi")
        private RestConfig restConfig;

        public Integer getFree_port_from() {
            return free_port_from;
        }

        public void setFree_port_from(Integer free_port_from) {
            this.free_port_from = free_port_from;
        }

        public Integer getFree_port_to() {
            return free_port_to;
        }

        public void setFree_port_to(Integer free_port_to) {
            this.free_port_to = free_port_to;
        }

        public RestConfig getRestConfig() {
            return restConfig;
        }

        public void setRestConfig(RestConfig restConfig) {
            this.restConfig = restConfig;
        }

        public String getPre_url() {
            return pre_url;
        }

        public void setPre_url(String pre_url) {
            this.pre_url = pre_url;
        }
    }



   }
