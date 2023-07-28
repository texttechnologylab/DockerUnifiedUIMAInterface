package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;

public class DUUISegmentationStrategyBySentence extends DUUISegmentationStrategyByAnnotation {
    public DUUISegmentationStrategyBySentence() {
        super();
        // TODO update params to sane defaults
        withSegmentationClass(Sentence.class);
        withMaxAnnotationsPerSegment(2);
        withMaxCharsPerSegment(100);
    }
}
