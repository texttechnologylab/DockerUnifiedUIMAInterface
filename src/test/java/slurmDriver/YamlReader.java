package slurmDriver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.bytedeco.libfreenect._freenect_context;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.adapter.GlobalAdvisorAdapterRegistry;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.ConfigManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.SlurmConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;


public class YamlReader {
    @Test
    public void reader() throws IOException {
        ConfigManager manager  = ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml"));
        SlurmConfig config = manager.getConfig();
        System.out.println(config.getContainerMode().getGrafanaPort());
    }
}