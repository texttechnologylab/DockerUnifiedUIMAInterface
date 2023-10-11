package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

/***
 * No document segmentation
 */
public class DUUISegmentationStrategyNone extends DUUISegmentationStrategy {
    @Override
    protected void initialize() throws UIMAException {
        // this class should not actually be used
    }

    @Override
    public JCas getNextSegment() {
        // this class should not actually be used
        return null;
    }

    @Override
    public void merge(JCas jCasSegment) {
        // this class should not actually be used
    }
}
