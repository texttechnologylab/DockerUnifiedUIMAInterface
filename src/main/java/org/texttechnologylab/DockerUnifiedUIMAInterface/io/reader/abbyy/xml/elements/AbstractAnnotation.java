package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.UIMAFramework;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.Logger;


abstract public class AbstractAnnotation implements IIntoAnnotation {
    public int start = 0;
    public int end = 0;

    public void setStart(int start) {
        this.start = start;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public Annotation into(JCas jCas) {
        return into(jCas, start, end);
    }

    public Annotation into(JCas jCas, int offset) {
        return into(jCas, start + offset, end + offset);
    }

    public Logger getLogger() {
        return UIMAFramework.getLogger(this.getClass());
    }
}
