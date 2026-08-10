package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.uima.tools.util.gui.SpringUtilities;

import java.util.List;

public class JobConfig {
    private String username;
    private String server;
    private String password;

    // 映射 YAML 中的 ssh_port 字段
    @JsonProperty("ssh_port")
    private String sshPort;

    @JsonProperty("work_dir")
    private String workDir;

    private List<String> jobs;

    public JobConfig() {}
    // Getters  Setters
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getServer() { return server; }
    public void setServer(String server) { this.server = server; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getSshPort() { return sshPort; }
    public void setSshPort(String sshPort) { this.sshPort = sshPort; }

    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public List<String> getJobs() { return jobs; }
    public void setJobs(List<String> jobs) { this.jobs = jobs; }

    @Override
    public String toString() {
        return "PipelineConfig{" +
                "username='" + username + '\'' +
                ", server='" + server + '\'' +
                ", password='[PROTECTED]'" +
                ", sshPort='" + sshPort + '\'' +
                ", jobs=" + jobs +
                '}';
    }

}
