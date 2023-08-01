package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.annotation.AnnotatorMetaData;
import org.texttechnologylab.annotation.SharedData;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.util.List;
import java.util.stream.Collectors;

public class RemoveAnnotations extends JCasAnnotator_ImplBase {

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        List<Annotation> annoList = JCasUtil.select(jCas, Annotation.class).stream().collect(Collectors.toList());
        List<AnnotatorMetaData> annoList2 = JCasUtil.select(jCas, AnnotatorMetaData.class).stream().collect(Collectors.toList());
        List<SharedData> annoList3 = JCasUtil.select(jCas, SharedData.class).stream().collect(Collectors.toList());
        List<ReproducibleAnnotation> annoReproducibleAnnotation = JCasUtil.select(jCas, ReproducibleAnnotation.class).stream().collect(Collectors.toList());

        annoList.stream().filter(a->{
            return !(a instanceof DocumentMetaData);
        }).forEach(annotation -> {
            annotation.removeFromIndexes();
        });

        annoList2.stream().forEach(annotation->{
            annotation.removeFromIndexes();
        });
        annoList3.stream().forEach(annotation->{
            annotation.removeFromIndexes();
        });
        annoReproducibleAnnotation.stream().forEach(annotation->{
            annotation.removeFromIndexes();
        });

    }
}
