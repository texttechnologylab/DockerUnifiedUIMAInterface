package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

import java.util.List;

public abstract class DUUISegmentationStrategy implements IDUUISegmentationStrategy {
    public abstract List<JCas> segment(JCas jCas) throws UIMAException;
    public abstract void combine(List<JCas> jCasSegmenteds, JCas jCas);
}
