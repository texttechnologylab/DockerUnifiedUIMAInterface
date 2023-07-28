package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

import java.io.Serializable;

public interface IDUUISegmentationStrategy extends Serializable {
    // Set a new JCas to be processed
    void initialize(JCas jCas) throws UIMAException;

    // Get final JCas with the generated annotations
    void finalize(JCas jCas);

    // Get next segment of the JCas
    JCas getNextSegment();

    // Merge the segmented JCas back into the output JCas
    void merge(JCas jCasSegment);

    // Add a segmentation user rule
    void addSegmentationRule(IDUUISegmentationRule rule);
}
