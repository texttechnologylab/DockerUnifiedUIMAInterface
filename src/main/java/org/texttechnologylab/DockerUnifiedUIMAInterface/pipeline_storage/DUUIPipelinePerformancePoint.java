package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import java.util.HashMap;
import java.util.Map;

public class DUUIPipelinePerformancePoint {
    private String _componentKey;
    private Long _durationSerialize;
    private Long _durationDeserialize;
    private Long _durationAnnotator;
    private Long _durationMutexWait;
    private Long _durationComponentTotal;

    public DUUIPipelinePerformancePoint(long durationSerialize, long durationDeserialize, long durationAnnotator, long durationMutexWait, long durationComponentTotal,
                                        String componentKey) {
        _componentKey = componentKey;

        _durationAnnotator = durationAnnotator;
        _durationComponentTotal = durationComponentTotal;
        _durationSerialize = durationSerialize;
        _durationDeserialize = durationDeserialize;
        _durationMutexWait = durationMutexWait;
    }

    public String getKey() {
        return _componentKey;
    }

    public Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("total", _durationComponentTotal);
        props.put("mutexsync", _durationMutexWait);
        props.put("annotator", _durationAnnotator);
        props.put("serialize", _durationSerialize);
        props.put("deserialize", _durationDeserialize);
        props.put("componentkey", _componentKey);
        return props;
    }
}
