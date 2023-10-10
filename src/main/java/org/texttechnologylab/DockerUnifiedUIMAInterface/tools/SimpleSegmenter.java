package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.annotation.AnnotatorMetaData;
import org.texttechnologylab.annotation.SharedData;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.util.List;
import java.util.stream.Collectors;

public class SimpleSegmenter extends JCasAnnotator_ImplBase {

    public static final String PARAM_SENTENCE_LENGTH = "length";

    @ConfigurationParameter(name = PARAM_SENTENCE_LENGTH, mandatory = false, defaultValue = "1000000")
    protected String length;

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        long lLength = Long.valueOf(length);

        String sText = jCas.getDocumentText();

        int iLength = sText.length();

        int iCount = 0;

        while(iCount<iLength){

            String sSubText = "";
            if(sText.length()>(int) (iCount+lLength)) {
                sSubText = sText.substring(iCount, (int) (iCount + lLength));
            }
            else{
                sSubText = sText;
            }
            int iLastPoint = sSubText.lastIndexOf(".");

            sSubText = sSubText.substring(0, iLastPoint<0 ? sSubText.length() : iLastPoint);

            Sentence pSentence = new Sentence(jCas);
            pSentence.setBegin(iCount);
            pSentence.setEnd(sSubText.length()+iCount);
            pSentence.addToIndexes();

            AnnotationComment ac = new AnnotationComment(jCas);
            ac.setKey("PSEUDOSENTENCE");
            ac.setReference(pSentence);
            ac.addToIndexes();

//            System.out.println(iCount+"\t"+sSubText.length());
            iCount = iCount+sSubText.length();

        }

        if(iCount<iLength){
            Sentence pSentence = new Sentence(jCas);
            pSentence.setBegin(iCount);
            pSentence.setEnd(iLength);
            pSentence.addToIndexes();
            AnnotationComment ac = new AnnotationComment(jCas);
            ac.setKey("PSEUDOSENTENCE");
            ac.setReference(pSentence);
            ac.addToIndexes();
        }


    }
}
