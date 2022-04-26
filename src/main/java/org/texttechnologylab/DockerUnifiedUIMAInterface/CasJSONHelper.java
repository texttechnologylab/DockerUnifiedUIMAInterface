package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CasJSONHelper {

    public static JSONObject toJSON(JCas pCas){



        JSONObject rObject = new JSONObject();

            JSONObject pMeta = new JSONObject();

            pMeta.put("text", pCas.getDocumentText());
            pMeta.put("language", pCas.getDocumentLanguage());

            rObject.put("meta", pMeta);

            Collection<Sentence> sentenceCollection = JCasUtil.select(pCas, Sentence.class);

            JSONArray sentenceArray = new JSONArray();

            if(sentenceCollection.size()>0){

                sentenceCollection.stream().forEach(s->{

                    JSONObject sObject = convertTOPtoJSON(s);
                    JSONArray sElements = new JSONArray();
                    JCasUtil.selectCovered(Token.class, s).forEach(t->{

                        JSONObject tObject =convertTOPtoJSON(t);
                        if(tObject!=null) {
                            sElements.put(tObject);
                        }

                    });
                    sObject.put("elements", sElements);
                    sentenceArray.put(sObject);
                });


            }
            else{

            }

            rObject.put("elements", sentenceArray);


        return rObject;

    }

    public static void appendAnnotations(JCas pTarget, JSONObject pValues){



    }



    public static JSONObject convertTOPtoJSON(TOP pTop){

        if(pTop==null){
            return null;
        }

        Set<String> blackList = new HashSet<>();
        blackList.add("uima.cas.Sofa");

        if(blackList.contains(pTop.getType().getName())){
            return null;
        }

        JSONObject rObject = new JSONObject();

            rObject.put("_type", pTop.getType().getName());

            pTop.getType().getFeatures().forEach(f->{

                Object pValue = null;
                if(f.getRange().isPrimitive()) {
                    switch (f.getRange().toString()) {

                        case "uima.cas.Integer":
                            pValue = Integer.valueOf(pTop.getFeatureValueAsString(f));
                            break;

                        case "uima.cas.String":
                            pValue = pTop.getFeatureValueAsString(f);
                            break;

                        case "uima.cas.Double":
                            pValue = Double.valueOf(pTop.getFeatureValueAsString(f));
                            break;

                        case "uima.cas.Float":
                            pValue = Float.valueOf(pTop.getFeatureValueAsString(f));
                            break;

                    }
                }
                else{

                    pValue = convertTOPtoJSON((TOP) pTop.getFeatureValue(f));

                }

                if(pValue!=null) {

                    rObject.put(f.getName(), pValue);
                }

            });

        return rObject;

    }

    @Test
    public void testCas() throws UIMAException, IOException {


        Set<String> sentences = new HashSet<>();
        sentences.add("Heute ist ein schöner Tag.");
        sentences.add("Aber leider habe ich Kopfschmerzen.");
        sentences.add("Zum Glück ist heute aber Berufsschule.");

        StringBuilder sb = new StringBuilder();
        sentences.forEach(s->{
            sb.append(s);
        });

        JCas pCas = JCasFactory.createText(sb.toString(), "de");

        sentences.forEach(s->{
            int iStart = pCas.getDocumentText().indexOf(s);
            int iEnd = iStart+s.length();
            Sentence pSentence = new Sentence(pCas, iStart, iEnd);
            pSentence.addToIndexes();

            for (String s1 : s.split(" ")) {
                int iSStart = s.indexOf(s1);
                iSStart=iSStart+iSStart;
                int iSEnd = iSStart+s1.length();

                Token pToken = new Token(pCas, iSStart, iSEnd);
                pToken.addToIndexes();

                Lemma pLemma = new Lemma(pCas, iSStart, iSEnd);
                pLemma.setValue(pToken.getCoveredText().toLowerCase());
                pLemma.addToIndexes();
                pToken.setLemma(pLemma);

            }

        });


        System.out.println(toJSON(pCas).toString(1));

    }



}
