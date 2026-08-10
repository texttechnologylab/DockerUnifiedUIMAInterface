package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;


import java.io.File;
import java.io.IOException;

public class ConfigManager {
    private static ConfigManager manager;
    private  ObjectMapper mapper;
    private  SlurmConfig config;

    private ConfigManager(File f) throws IOException {
        mapper = new ObjectMapper(new YAMLFactory());
        config = mapper.readValue(f, SlurmConfig.class);
    }

    public static synchronized ConfigManager getManager(File f) throws IOException {
        if (manager!=null){return manager;}
        else {
            manager = new ConfigManager(f);
            return manager;
        }
    }

    public SlurmConfig getConfig() {
        return config;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }
}
