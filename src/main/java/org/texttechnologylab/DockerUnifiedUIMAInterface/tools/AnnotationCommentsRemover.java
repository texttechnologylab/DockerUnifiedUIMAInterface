package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.AnnotationBase;
import org.apache.uima.jcas.cas.TOP;
import org.texttechnologylab.annotation.AnnotationComment;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Set;

public class AnnotationCommentsRemover extends JCasAnnotator_ImplBase {

    public static final String PARAM_ANNOTATION_KEY = "key";

    @ConfigurationParameter(name = PARAM_ANNOTATION_KEY, mandatory = true)
    protected String key;

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        Set<TOP> acRemove = new HashSet<>();

        JCasUtil.select(jCas, AnnotationComment.class).stream().forEach(ac->{
            if(ac.getKey().equalsIgnoreCase(key)){
                acRemove.add(ac);
                acRemove.add(ac.getReference());
            }
        });

        acRemove.stream().forEach(ac->{
            ac.removeFromIndexes();
        });

    }
}
