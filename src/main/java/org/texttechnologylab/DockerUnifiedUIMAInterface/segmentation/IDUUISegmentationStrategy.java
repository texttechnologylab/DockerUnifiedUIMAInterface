package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

import java.util.List;

public interface IDUUISegmentationStrategy {
    List<JCas> segment(JCas jCas) throws UIMAException;
    void combine(List<JCas> jCasSegmenteds, JCas jCas);
}
