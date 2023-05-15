package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.jcas.JCas;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

/***
 * No document segmentation, this just uses the full input JCas.
 */
public class DUUISegmentationStrategyNone extends DUUISegmentationStrategy {
    @Override
    protected void initialize() {
        // nothing to do
    }

    @Override
    public void merge(JCas jCasSegment) {
        // nothing to merge as we did not segment cas in the first place, just replace with the segmented
        jCas = jCasSegment;
    }

    // The iterator only returns the input JCas
    class DUUISegmentationStrategyNoneIterator implements Iterator<JCas> {
        private boolean hasMore = true;

        @Override
        public boolean hasNext() {
            return hasMore;
        }

        @Override
        public JCas next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            hasMore = false;
            return DUUISegmentationStrategyNone.this.jCas;
        }
    }

    @NotNull
    @Override
    public Iterator<JCas> iterator() {
        return new DUUISegmentationStrategyNoneIterator();
    }
}
