package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.xml.sax.Attributes;

abstract public class AbstractStructuralAnnotation extends AbstractStructuralElement implements IIntoAnnotation {
    public int start = 0;
    public int end = 0;

    public AbstractStructuralAnnotation(Attributes attributes) {
        super(attributes);
    }

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

    public boolean insideBoundingBox(int top, int right, int bottom, int left) {
        return this.top >= top && this.right <= right && this.bottom <= bottom && this.left >= left;
    }
}
