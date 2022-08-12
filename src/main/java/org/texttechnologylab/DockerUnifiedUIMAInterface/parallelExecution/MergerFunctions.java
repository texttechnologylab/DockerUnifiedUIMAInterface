package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelExecution;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasCopier;

import java.util.Collection;

public class MergerFunctions {

    /**
     * copy single annotation from srcCas to destCas described by annotationType
     */
    public static void merge(JCas srcCas, JCas destCas, int annotationType) {
        CasCopier casCopier = new CasCopier(srcCas.getCas(), destCas.getCas());
        destCas.removeAllExcludingSubtypes(annotationType);

        Class<? extends TOP> annotationClass = JCasRegistry.getClassForIndex(annotationType);
        JCasUtil.select(srcCas, annotationClass).forEach(annotation -> {
            destCas.addFsToIndexes(casCopier.copyFs(annotation));
        });
    }


    /**
     * copy multiple annotations from srcCas to destCas described by List annotationTypes
     */
    public static void merge(JCas srcCas, JCas destCas, Collection<Integer> annotationTypes) {
        CasCopier casCopier = new CasCopier(srcCas.getCas(), destCas.getCas());
        for(Integer annotationType:annotationTypes) {
            destCas.removeAllExcludingSubtypes(annotationType);

            Class<? extends TOP> annotationClass = JCasRegistry.getClassForIndex(annotationType);
            JCasUtil.select(srcCas, annotationClass).forEach(annotation -> {
                destCas.addFsToIndexes(casCopier.copyFs(annotation));
            });
        }
    }

    /**
     * copy all Annotations form srcCas to destCas
     */
    public static void mergeAll(JCas srcCas, JCas destCas){
        CasCopier casCopier = new CasCopier(srcCas.getCas(), destCas.getCas());
        JCasUtil.select(srcCas, Annotation.class).forEach(annotation -> {
                destCas.addFsToIndexes(casCopier.copyFs(annotation));
        });
    }
}
