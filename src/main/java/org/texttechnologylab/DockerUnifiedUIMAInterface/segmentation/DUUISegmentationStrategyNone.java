package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasCopier;

/***
 * No document segmentation, this just uses the full input JCas.
 */
public class DUUISegmentationStrategyNone extends DUUISegmentationStrategy {
    private boolean hasMore;

    @Override
    protected void initialize() {
        hasMore = true;
    }

    @Override
    public JCas getNextSegment() {
        // Return the input JCas only once
        if (!hasMore) {
            return null;
        }
        hasMore = false;
        return DUUISegmentationStrategyNone.this.jCasInput;
    }

    @Override
    public void merge(JCas jCasSegment) {
        // nothing to merge as we did not segment cas in the first place,
        // just replace with the segmented
        jCasOutput.reset();
        CasCopier.copyCas(jCasSegment.getCas(), jCasOutput.getCas(), true);
    }
}
