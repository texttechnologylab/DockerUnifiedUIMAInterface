package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.util.HashMap;

public class IDUUIPipelineComponent {
    private HashMap<String, String> _options;

    public IDUUIPipelineComponent() {
        _options = new HashMap<>();
    }

    public IDUUIPipelineComponent(IDUUIPipelineComponent other) {
        _options = other._options;
    }

    public void setOption(String key, String value) {
        _options.put(key, value);
    }

    public String getOption(String key) {
        return _options.get(key);
    }

    public boolean hasOption(String key) {
        return _options.containsKey(key);
    }

    public String removeOption(String key) {
        return _options.remove(key);
    }
}
