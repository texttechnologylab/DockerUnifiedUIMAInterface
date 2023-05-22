package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.jcas.JCas;

public abstract class DUUISegmentationStrategy implements Iterable<JCas> {
    // The current JCas to be processed
    protected JCas jCas;

    // Set the JCas to be processed, this should reset all state
    public void setJCas(JCas jCas) {
        this.jCas = jCas;
        this.initialize();
    }

    // Initialize the state of the segmentation strategy,
    // this is called when a new JCas is set
    protected abstract void initialize();

    // Merge the segmented JCas back into the original JCas
    public abstract void merge(JCas jCasSegment);
}
