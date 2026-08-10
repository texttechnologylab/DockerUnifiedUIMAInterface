package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;

import java.io.File;
import java.io.IOException;
public class JobReader {
    String path;
    public JobReader(String filepath) {
        this.path = filepath;
    }

    public JobConfig loadConfig() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        return mapper.readValue(new File(this.path), JobConfig.class);
    }

    public static void main(String[] args) {
        try {
            // assuem filename config.yml
            JobReader j  = new JobReader("src/main/resources/slurmDriverConf/jobs.yaml");
            JobConfig config = j.loadConfig();

            System.out.println("=== Jackson YAML parse ===");
            System.out.println("Username: " + config.getUsername());
            System.out.println("Server: " + config.getServer());
            System.out.println("SSH Port: " + config.getSshPort());
            System.out.println("Jobs: " + (config.getJobs()).get(0));

        } catch (IOException e) {
            System.err.println("Jackson parse error:");
            e.printStackTrace();
        }
    }


}
