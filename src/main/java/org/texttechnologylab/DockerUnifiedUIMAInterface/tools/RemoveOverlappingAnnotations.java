package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RemoveOverlappingAnnotations extends JCasAnnotator_ImplBase {

    public static final String PARAM_TYPE_LIST = "types";

    @ConfigurationParameter(name = PARAM_TYPE_LIST, mandatory = true)
    protected String types;

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        String[] sTypes = types.split(",");

        List<Class> pClasses = new ArrayList<>();

        for (String sType : sTypes) {
            try {
                pClasses.add(Class.forName(sType));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        Set<Annotation> deleteSet = new HashSet();

        Set<String> boundings = new HashSet<>(0);

        for (Class pClass : pClasses) {
            JCasUtil.select(jCas, pClass).stream().forEach(a -> {
                if (a instanceof Annotation) {
                    Annotation pA = (Annotation) a;
                    boundings.add(pA.getBegin() + "-" + pA.getEnd());
                }
            });
        }


//        System.out.println("Stop");

    }
}
