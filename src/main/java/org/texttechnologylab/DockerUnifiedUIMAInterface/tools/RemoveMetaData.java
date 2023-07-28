package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.AnnotatorMetaData;

import java.util.List;
import java.util.stream.Collectors;

public class RemoveMetaData extends JCasAnnotator_ImplBase {

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        List<AnnotatorMetaData> annoList2 = JCasUtil.select(jCas, AnnotatorMetaData.class).stream().collect(Collectors.toList());

        annoList2.stream().forEach(annotation->{
            annotation.removeFromIndexes();
        });

    }
}
