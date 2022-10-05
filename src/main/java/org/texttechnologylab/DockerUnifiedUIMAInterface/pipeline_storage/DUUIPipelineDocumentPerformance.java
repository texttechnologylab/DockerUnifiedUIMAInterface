package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import com.arangodb.entity.BaseDocument;
import org.apache.uima.jcas.JCas;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class DUUIPipelineDocumentPerformance {
    private Vector<DUUIPipelinePerformancePoint> _points;
    private String _runKey;
    private Long _durationTotalSerialize;
    private Long _durationTotalDeserialize;
    private Long _durationTotalAnnotator;
    private Long _durationTotalMutexWait;
    private Long _durationTotal;
    private Integer _documentSize;
    private Long _documentWaitTime;

    public DUUIPipelineDocumentPerformance(String runKey, long waitDocumentTime, JCas jc) {
        _points = new Vector<>();
        _runKey = runKey;

        _documentWaitTime = waitDocumentTime;
        _durationTotalDeserialize = 0L;
        _durationTotalSerialize = 0L;
        _durationTotalAnnotator = 0L;
        _durationTotalMutexWait = 0L;
        _durationTotal = 0L;
        if(jc.getDocumentText()!=null) {
            _documentSize = jc.getDocumentText().length();
        }
        else{
            _documentSize = -1;
        }

    }

    public String getRunKey() {
        return _runKey;
    }

    public Vector<DUUIPipelinePerformancePoint> getPerformancePoints() {
        return _points;
    }

    public void addData(long durationSerialize, long durationDeserialize, long durationAnnotator, long durationMutexWait, long durationComponentTotal, String componentKey, long serializeSize, JCas jc) {
        _durationTotalDeserialize += durationDeserialize;
        _durationTotalSerialize += durationSerialize;
        _durationTotalAnnotator += durationAnnotator;
        _durationTotalMutexWait += durationMutexWait;
        _durationTotal += durationComponentTotal;
        _points.add(new DUUIPipelinePerformancePoint(durationSerialize,durationDeserialize,durationAnnotator,durationMutexWait,durationComponentTotal,componentKey,serializeSize, jc));
    }

    public long getDocumentWaitTime() {
        return _documentWaitTime;
    }

    public long getTotalTime() {
        return _durationTotal+_documentWaitTime;
    }

    public long getDocumentSize() {
        return _documentSize;
    }

    public Vector<BaseDocument> generateComponentPerformance(String docKey) {
        Vector<BaseDocument> docs = new Vector<>();
        for(DUUIPipelinePerformancePoint point : _points) {
            Map<String, Object> props = new HashMap<>();
            props.put("run", _runKey);
            props.put("compkey", point.getKey());
            props.put("performance",point.getProperties());
            props.put("docsize",_documentSize);
            BaseDocument doc = new BaseDocument();
            doc.setProperties(props);

            docs.add(doc);
        }
        return docs;
    }

    public BaseDocument toArangoDocument() {
        BaseDocument doc = new BaseDocument();
        Map<String,Object> props = new HashMap<>();

        props.put("pipelineKey",_runKey);
        props.put("total",_durationTotal);
        props.put("mutexsync",_durationTotalMutexWait);
        props.put("annotator",_durationTotalAnnotator);
        props.put("serialize",_durationTotalSerialize);
        props.put("deserialize",_durationTotalDeserialize);
        props.put("docsize",_documentSize);
        doc.setProperties(props);
        return doc;
    }
}
