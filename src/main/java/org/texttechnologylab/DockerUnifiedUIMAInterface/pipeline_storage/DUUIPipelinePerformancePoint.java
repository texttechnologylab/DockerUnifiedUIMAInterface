package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

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
    private Long _serializedSize;

    private String error;
    private String document;

    public DUUIPipelinePerformancePoint(long durationSerialize, long durationDeserialize, long durationAnnotator, long durationMutexWait, long durationComponentTotal,
                                        String componentKey, long serializedSize, JCas jc, String error, String document) {
        _componentKey = componentKey;

        _durationAnnotator = durationAnnotator;
        _durationComponentTotal = durationComponentTotal;
        _durationSerialize = durationSerialize;
        _durationDeserialize = durationDeserialize;
        _durationMutexWait = durationMutexWait;
        try {
            _numberAnnotations = JCasUtil.select(jc, TOP.class).stream().count();
        }
        catch (Exception e){
            _numberAnnotations=0l;
        }
        try {
            _documentSize = Long.valueOf(jc.getDocumentText().length());
        }
        catch (Exception e){
            _documentSize=-1l;
        }
        _serializedSize = serializedSize;

        this.document = document;

        this.error = error;
    }

    public String getError() {
        return error;
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

    public Long getSerializedSize() {
        return _serializedSize;
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

    public String getDocument() {
        return document;
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
