package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

public interface IIntoAnnotation {
    void setStart(int start);

    void setEnd(int end);

    Annotation into(JCas jcas, int start, int end);

    Annotation into(JCas jCas);

    Annotation into(JCas jCas, int offset);
}
