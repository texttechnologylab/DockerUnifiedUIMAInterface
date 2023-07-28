package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.jcas.JCas;

import java.io.Serializable;

public interface IDUUISegmentationRule extends Serializable {
    /***
     * Can the CAS be segmented at this position?
     * Note: Where this rule is applied depends on the implementation of the segmentation strategy, as well as how the output is used.
     *
     * @param resultRuleBefore Whether the previous rule led to a segmentation at this position
     * @param begin The begin offset of the current segment
     * @param end The possible end offset of the current segment
     * @param jCas The JCas to be segmented
     * @param segmentationStrategy The segmentation strategy that is used
     * @return true, if the CAS can be segmented at this position else false
     */
    boolean canSegment(boolean resultRuleBefore, int begin, int end, JCas jCas, IDUUISegmentationStrategy segmentationStrategy);
}
