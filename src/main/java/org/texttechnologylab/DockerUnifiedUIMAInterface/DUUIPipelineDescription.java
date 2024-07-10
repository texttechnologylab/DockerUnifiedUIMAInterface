package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Vector;

/**
 * Description of a pipeline for the DUUI composer that holds all components.
 */
public class DUUIPipelineDescription {
    private Vector<DUUIPipelineAnnotationComponent> _components;

    /**
     * Constructor
     * @param components List of components of the pipeline.
     */
    public DUUIPipelineDescription(Vector<DUUIPipelineAnnotationComponent> components) {
        _components = components;
    }

    /**
     * Get the components of the pipeline.
     * @return List of components.
     */
    public Vector<DUUIPipelineAnnotationComponent> getComponents() {
        return _components;
    }

    public static DUUIPipelineDescription fromJCas(JCas jc) throws CompressorException, URISyntaxException, IOException {
        Vector<DUUIPipelineAnnotationComponent> components = new Vector<>();
        for(ReproducibleAnnotation ann : JCasUtil.select(jc,ReproducibleAnnotation.class)) {
            components.add(new DUUIPipelineAnnotationComponent(ann));
        }

        components.sort((a,b) -> {
            Long timeA = a.getAnnotation().getTimestamp();
            Long timeB = b.getAnnotation().getTimestamp();
            return timeA.compareTo(timeB);
        });
        return new DUUIPipelineDescription(components);
    }
}
