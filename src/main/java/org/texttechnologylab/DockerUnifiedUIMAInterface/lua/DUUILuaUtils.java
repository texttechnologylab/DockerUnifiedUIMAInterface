package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.uima.cas.Feature;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.jcas.JCas;

/**
 * Auxiliary class for the UTF16 problem
 *
 * @author Daniel Baumartz
 */
public class DUUILuaUtils {
    // TODO Different from Luas "string.len"?
    static public int getDocumentTextLength(JCas jCas) {
        return jCas.getDocumentText().length();
    }

    public static AnnotationFS createAnnotationInstance(String sClassName, JCas jCas) {
        AnnotationFS rObject = jCas.getCas().createFS(jCas.getTypeSystem().getType(sClassName));
        jCas.addFsToIndexes(rObject);
        return rObject;
    }

    public static void addAnnotation(AnnotationFS a, String sMethod, String sValue) {
        Feature pFeature = a.getType().getFeatureByBaseName(sMethod.toLowerCase());
        a.setFeatureValueFromString(pFeature, sValue);
    }

    public static void addAnnotation(AnnotationFS a, String sMethod, AnnotationFS tFeature) {
        Feature pFeature = a.getType().getFeatureByBaseName(sMethod.toLowerCase());
        a.setFeatureValue(pFeature, tFeature);
    }

}
