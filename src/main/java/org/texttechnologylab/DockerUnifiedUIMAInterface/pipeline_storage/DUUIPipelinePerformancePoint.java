package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;

import java.util.HashMap;
import java.util.Map;

public class DUUIPipelinePerformancePoint {
    private String _componentKey;
    private Long _durationSerialize;
    private Long _durationDeserialize;
    private Long _durationAnnotator;
    private Long _durationMutexWait;
    private Long _durationComponentTotal;
    private Long _numberAnnotations;
    private Long _documentSize;

    public DUUIPipelinePerformancePoint(long durationSerialize, long durationDeserialize, long durationAnnotator, long durationMutexWait, long durationComponentTotal,
                                        String componentKey, JCas jc) {
        _componentKey = componentKey;

        _durationAnnotator = durationAnnotator;
        _durationComponentTotal = durationComponentTotal;
        _durationSerialize = durationSerialize;
        _durationDeserialize = durationDeserialize;
        _durationMutexWait = durationMutexWait;
        _numberAnnotations = JCasUtil.select(jc, TOP.class).stream().count();
        _documentSize = Long.valueOf(jc.getDocumentText().length());
    }

    public String getKey() {
        return _componentKey;
    }

    public Long getDurationAnnotator() {
        return _durationAnnotator;
    }

    public Long getDurationComponentTotal() {
        return _durationComponentTotal;
    }

    public Long getDurationSerialize() {
        return _durationSerialize;
    }

    public Long getDurationDeserialize() {
        return _durationDeserialize;
    }

    public Long getDurationMutexWait() {
        return _durationMutexWait;
    }

    public Long getNumberOfAnnotations() {
        return _numberAnnotations;
    }

    public Long getDocumentSize() {
        return _documentSize;
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
