package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasCopier;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIInstantiatedPipelineComponent;

import java.util.Collection;

public class MergerFunctions {

    /**
     * copy single annotation from srcCas to destCas described by annotationTypeName
     */
    public static void merge(JCas srcCas, JCas destCas, String annotationTypeName) throws ClassNotFoundException {
        Class<? extends TOP> annotationClass = (Class<? extends TOP>) IDUUIInstantiatedPipelineComponent.class.getClassLoader().loadClass(annotationTypeName);
        CasCopier casCopier = new CasCopier(srcCas.getCas(), destCas.getCas());
        Collection<TOP> annotations = JCasUtil.select(srcCas, (Class<TOP>) annotationClass);
        for (TOP annotation : annotations) {
            destCas.addFsToIndexes(casCopier.copyFs(annotation));
        }
    }


    /**
     * copy single annotation from srcCas to destCas described by annotationType
     */
    public static void merge(JCas srcCas, JCas destCas, int annotationType) {
        CasCopier casCopier = new CasCopier(srcCas.getCas(), destCas.getCas());
        destCas.removeAllExcludingSubtypes(annotationType);

        Class<? extends TOP> annotationClass = JCasRegistry.getClassForIndex(annotationType);
        JCasUtil.select(srcCas, annotationClass).forEach(annotation -> destCas.addFsToIndexes(casCopier.copyFs(annotation)));
    }


    /**
     * copy all Annotations form srcCas to destCas
     */
    public static void mergeAll(JCas srcCas, JCas destCas) {
//        CasCopier casCopier = new CasCopier(srcCas.getCas(), destCas.getCas());
//        casCopier.copyCasView(srcCas,destCas,true);
//        for(TOP annotation : JCasUtil.selectAll(srcCas)) {
//            destCas.addFsToIndexes(casCopier.copyFs(annotation));
//        }
        CasCopier.copyCas(srcCas.getCas(), destCas.getCas(), destCas.getSofa() == null);

    }
}