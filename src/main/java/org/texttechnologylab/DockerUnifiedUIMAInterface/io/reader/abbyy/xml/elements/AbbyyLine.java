package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.annotation.ocr.abbyy.Line;
import org.xml.sax.Attributes;

public class AbbyyLine extends AbstractStructuralAnnotation {
    private final int baseline;

    // FIXME: Evaluate whether there are lines with multiple <formatting> tags!
    public AbbyyFormat format;

    public AbbyyLine(Attributes attributes) {
        super(attributes);
        this.baseline = Utils.parseInt(attributes.getValue("baseline"));
    }

    @Override
    public Line into(JCas jcas, int start, int end) {
        Line line = new Line(jcas, start, end);
        line.setBaseline(baseline);
        if (format != null) {
            line.setFormat(format.into(jcas, start, end));
        }
        return line;
    }

    public void setFormat(AbbyyFormat format) {
        this.format = format;
        if (this.format != null) {
            this.format.setStart(start);
            this.format.setEnd(end);
        }
    }

    @Override
    public void setEnd(int end) {
        super.setEnd(end);
        if (format != null) {
            format.setEnd(end);
        }
    }
}
