package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;


import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.SpacyAnnotatorMetaData;

/**
 * JCasCollectionReader to read a single article from multiple XML files.
 */
public class AnnotationRemover extends JCasAnnotator_ImplBase {

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {

        for (SpacyAnnotatorMetaData spacyAnnotatorMetaData : JCasUtil.select(aJCas, SpacyAnnotatorMetaData.class)) {
            spacyAnnotatorMetaData.removeFromIndexes();
        }

    }


}
