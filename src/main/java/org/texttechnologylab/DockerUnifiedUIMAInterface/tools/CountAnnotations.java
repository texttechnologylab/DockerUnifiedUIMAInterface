package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.io.JCasFileWriter_ImplBase;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.resource.ResourceInitializationException;

import java.util.HashMap;
import java.util.Map;

public class CountAnnotations extends JCasFileWriter_ImplBase {

    static Map<String, Integer> annoCount = new HashMap();

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        annoCount.clear();
    }

    @Override
    public void destroy() {

        int corpusMax = 1711285;
        int corpusSample = 2000;

        System.out.println("Annotations: ");
            annoCount.keySet().stream().sorted().forEach(k->{

                int iAmount = annoCount.get(k);
                int sampleAmount = iAmount / corpusSample;
                int maxAmount = sampleAmount * corpusMax;

//                System.out.println("Sample: "+k+"\t"+iAmount);
                System.out.println("Calc: "+k+"\t"+maxAmount);

            });

            super.destroy();
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        JCasUtil.select(jCas, TOP.class).forEach(a->{
            String sType = a.getType().getName();

            if(a instanceof POS){
                sType = ((POS)a).getPosValue();
            }

            int iCount =0;

            if(annoCount.containsKey(sType)){
                iCount = annoCount.get(sType);
            }
            iCount++;

            annoCount.put(sType, iCount);

        });

    }
}
