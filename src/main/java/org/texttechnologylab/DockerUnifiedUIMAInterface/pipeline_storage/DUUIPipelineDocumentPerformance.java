package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.apache.uima.jcas.JCas;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

public class DUUIPipelineDocumentPerformance {
    private Vector<DUUIPipelinePerformancePoint> _points;
    private String _runKey;
    private Long _durationTotalSerialize;
    private Long _durationTotalDeserialize;
    private Long _durationTotalAnnotator;
    private Long _durationTotalMutexWait;
    private Long _durationTotal;
    private Integer _documentSize;

    public DUUIPipelineDocumentPerformance(String runKey, JCas jc) {
        _points = new Vector<>();
        _runKey = runKey;

        _durationTotalDeserialize = 0L;
        _durationTotalSerialize = 0L;
        _durationTotalAnnotator = 0L;
        _durationTotalMutexWait = 0L;
        _durationTotal = 0L;
        _documentSize = jc.getDocumentText().length();
    }

    public String getRunKey() {
        return _runKey;
    }


    public void addData(long durationSerialize, long durationDeserialize, long durationAnnotator, long durationMutexWait, long durationComponentTotal, String componentKey) {
        _durationTotalDeserialize += durationDeserialize;
        _durationTotalSerialize += durationSerialize;
        _durationTotalAnnotator += durationAnnotator;
        _durationTotalMutexWait += durationMutexWait;
        _durationTotal += durationComponentTotal;
        _points.add(new DUUIPipelinePerformancePoint(durationSerialize,durationDeserialize,durationAnnotator,durationMutexWait,durationComponentTotal,componentKey));
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
