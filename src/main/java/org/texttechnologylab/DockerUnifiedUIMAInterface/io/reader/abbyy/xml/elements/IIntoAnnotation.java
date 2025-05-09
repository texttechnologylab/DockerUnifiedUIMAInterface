package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

public interface IIntoAnnotation {
    public void setStart(int start);

    public void setEnd(int end);

    Annotation into(JCas jcas, int start, int end);

    public Annotation into(JCas jCas);

    public Annotation into(JCas jCas, int offset);
}
