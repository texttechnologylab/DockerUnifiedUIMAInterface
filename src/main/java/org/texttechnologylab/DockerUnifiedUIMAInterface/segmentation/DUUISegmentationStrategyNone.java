package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.jcas.JCas;

import java.util.Collections;
import java.util.List;

/***
 * Not document segmentation.
 */
public class DUUISegmentationStrategyNone extends DUUISegmentationStrategy {
    @Override
    public List<JCas> segment(JCas jCas) {
        return Collections.singletonList(jCas);
    }

    @Override
    public void combine(List<JCas> jCasSegmenteds, JCas jCas) {
        jCas = jCasSegmenteds.get(0);
    }
}
