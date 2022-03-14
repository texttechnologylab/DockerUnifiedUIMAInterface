import java.util.HashMap;

public class IDUUIPipelineComponent {
    private HashMap<String,String> _options;

    IDUUIPipelineComponent() {
        _options = new HashMap<>();
    }

    IDUUIPipelineComponent(IDUUIPipelineComponent other) {
        _options = other._options;
    }

    public void setOption(String key,String value) {
        _options.put(key,value);
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
