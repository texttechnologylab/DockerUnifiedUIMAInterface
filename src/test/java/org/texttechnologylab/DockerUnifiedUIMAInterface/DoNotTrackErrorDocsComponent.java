package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.jcas.JCas;

/**
 * A test component that fails every second document.
 */
public class DoNotTrackErrorDocsComponent extends JCasAnnotator_ImplBase {
    private int counter = 0;

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        counter++;
        if (counter % 2 == 0) {
            throw new AnalysisEngineProcessException(new Exception("Processing failed for some reason."));
        }
    }
}