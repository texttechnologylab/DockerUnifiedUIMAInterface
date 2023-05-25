package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

public abstract class DUUISegmentationStrategy implements Iterable<JCas> {
    // The current JCas to be processed, this should not be modified
    protected JCas jCasInput;

    // The final JCas where the generated annotations are merged into
    protected JCas jCasOutput;

    // Set the JCas to be processed, this should reset all state
    public void setJCas(JCas jCas) throws UIMAException {
        this.jCasInput = jCas;
        this.initialize();
    }

    // Get final JCas with the generated annotations
    public JCas getJCas() {
        return jCasOutput;
    }

    // Initialize the state of the segmentation strategy,
    // this is called automatically when a new JCas is set
    protected abstract void initialize() throws UIMAException;

    // Merge the segmented JCas back into the output JCas
    public abstract void merge(JCas jCasSegment);
}
