package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;


import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Feature;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;

/**
 * JCasCollectionReader to read a single article from multiple XML files.
 */
public class AnnotationMapper extends JCasAnnotator_ImplBase {

    public static final String PARAM_MAPPING = "mapping";
    @ConfigurationParameter(name = PARAM_MAPPING, mandatory = true, defaultValue = "[{\"org.texttechnologylab.annotation.type.Person_HumanBeing\": \"de.tudarmstadt.ukp.dkpro.core.api.ner.type.Person\"}]")
    protected String mapping;

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {

        JSONArray pMapping = new JSONArray(mapping);

        for (int a = 0; a < pMapping.length(); a++) {

            JSONObject pObject = pMapping.getJSONObject(a);
            String sSource = pObject.keySet().stream().findFirst().get();
            String sTarget = pObject.getString(sSource);

            try {
                Class pSourceClass = Class.forName(sSource);
                Class pTargetClass = Class.forName(sTarget);
                JCasUtil.select(aJCas, pSourceClass).stream().forEach(sourceClass -> {

                    TOP pAnnotation = ((TOP) sourceClass);

                    try {
                        TOP fs = (TOP) pTargetClass.getConstructor(JCas.class).newInstance(aJCas);
                        for (Feature feature : fs.getType().getFeatures()) {
                            try {
                                fs.setFeatureValue(feature, pAnnotation.getFeatureValue(feature));
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                            }
                        }
                        fs.addToIndexes();
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                    } catch (InstantiationException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }


                });

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }


        }


    }


}
