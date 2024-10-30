package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.FeatureStructureImplC;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.AnnotatorMetaData;
import org.texttechnologylab.annotation.SpacyAnnotatorMetaData;

import java.util.List;
import java.util.stream.Collectors;

public class RemoveSmallSentences extends JCasAnnotator_ImplBase {

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        List<Sentence> annoList2 = JCasUtil.select(jCas, Sentence.class).stream().toList();

        annoList2.stream().filter(annotation->{
            return annotation.getCoveredText().length()<2;
        }).forEach(FeatureStructureImplC::removeFromIndexes);

        List<SpacyAnnotatorMetaData> annoListS = JCasUtil.select(jCas, SpacyAnnotatorMetaData.class).stream().toList();

        annoListS.forEach(FeatureStructureImplC::removeFromIndexes);
    }
}
