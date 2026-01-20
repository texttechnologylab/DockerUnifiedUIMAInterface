package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.FeatureStructureImplC;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;

import java.util.List;

public class RemoveSmallSentences extends JCasAnnotator_ImplBase {

    public static final String PARAM_LENGTH = "length";
    @ConfigurationParameter(name = PARAM_LENGTH, mandatory = false, defaultValue = "5")
    protected int length;


    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        List<Sentence> annoList2 = JCasUtil.select(jCas, Sentence.class).stream().toList();

        annoList2.stream().filter(annotation -> {
            return annotation.getCoveredText().length() < length;
        }).forEach(FeatureStructureImplC::removeFromIndexes);

        annoList2.forEach(FeatureStructureImplC::removeFromIndexes);
    }
}
