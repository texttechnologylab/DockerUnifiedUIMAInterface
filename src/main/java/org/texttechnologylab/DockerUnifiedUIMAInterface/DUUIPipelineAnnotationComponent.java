package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.io.IOException;
import java.net.URISyntaxException;

public class DUUIPipelineAnnotationComponent {
    private ReproducibleAnnotation _annotation;
    private DUUIPipelineComponent _decoded;

    public DUUIPipelineAnnotationComponent(ReproducibleAnnotation ann) throws CompressorException, URISyntaxException, IOException {
        _annotation = ann;
        _decoded = DUUIPipelineComponent.fromEncodedJson(ann.getDescription(), ann.getCompression());
    }

    public DUUIPipelineComponent getComponent() {
        return _decoded;
    }

    public ReproducibleAnnotation getAnnotation() {
        return _annotation;
    }
}
