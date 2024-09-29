package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulation of a pipeline component
 * @author Alexander Leonhardt
 */
public class IDUUIPipelineComponent {
    /**
     * Options of the Component
     */
    private HashMap<String, String> _options;
    /**
     * Params of the Component
     */
    private HashMap<String,String> _parameters;

    /**
     * default constructor
     */
    public IDUUIPipelineComponent() {
        _options = new HashMap<>();
    }

    /**
     * constructor
     * @param options
     */
    public IDUUIPipelineComponent(Map<String,Object> options) {
        _options = new HashMap<>();
        for(Map.Entry<String,Object> entry : options.entrySet()) {
            _options.put(entry.getKey(),String.valueOf(entry.getValue()));
        }
    }

    /**
     * Adding a key-value pair as a parameter
     * @param key
     * @param value
     * @return
     */
    public IDUUIPipelineComponent withParameter(String key, String value) {
        //TODO: Add this to the ArangoDB backend!!! Only options are serialized at the moment
        if(_parameters==null) {
            _parameters = new HashMap<>();
        }
        _parameters.put(key,value);
        return this;
    }

    /**
     * Return of all set parameters
     * @return
     */
    public Map<String,String> getParameters() {
        return _parameters;
    }

    /**
     * Duplication of the options of an existing component
     * @param other
     */
    public IDUUIPipelineComponent(IDUUIPipelineComponent other) {
        _options = other._options;
    }

    /**
     * Adding a key-value pair as an option
     * @param key
     * @param value
     */
    public void setOption(String key, String value) {
        _options.put(key, value);
    }

    /**
     * Get specific value of an option-parameter
     * @param key
     * @return
     */
    public String getOption(String key) {
        return _options.get(key);
    }

    /**
     * Check whether a key exists as an option.
     * @param key
     * @return
     */
    public boolean hasOption(String key) {
        return _options.containsKey(key);
    }

    /**
     * Return of all configured options
     * @return
     */
    public Map<String,String> getOptions() {return _options;}

    /**
     * Set option "name"
     * @param name
     */
    public void withName(String name) {
        setOption("name",name);
    }

    /**
     * Get option "name"
     * @return
     */
    public String getName() {
        return getOption("name");
    }

    /**
     * Remove an option by using a key
     * @param key
     * @return
     */
    public String removeOption(String key) {
        return _options.remove(key);
    }
}
