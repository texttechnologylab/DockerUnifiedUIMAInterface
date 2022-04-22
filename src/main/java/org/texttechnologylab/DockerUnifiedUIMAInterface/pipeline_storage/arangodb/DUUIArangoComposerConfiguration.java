package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.arangodb;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public class DUUIArangoComposerConfiguration extends BaseDocument {
    public DUUIArangoComposerConfiguration(String name, int workers) {
        super(name);
        Map<String,Object> props = new HashMap<String,Object>();
        props.put("workers",workers);
        props.put("name",name);
        setProperties(props);
    }
}

class DUUIArangoPipelineComponent extends BaseDocument {
    public DUUIArangoPipelineComponent(String hash, Map<String,Object> props) {
        super(hash);
        setProperties(props);
    }
}

class DUUIArangoPipelineEdge {
    private BaseEdgeDocument _edge;

    public DUUIArangoPipelineEdge(String from, String to, String pipelinename) {
        Map<String,Object> props = new HashMap<String,Object>();
        props.put("duui_reserved_pipeline",pipelinename);
        _edge = new BaseEdgeDocument();
        _edge.setFrom(from);
        _edge.setTo(to);
        _edge.setProperties(props);
    }

    public BaseEdgeDocument edge() {
        return _edge;
    }
}
