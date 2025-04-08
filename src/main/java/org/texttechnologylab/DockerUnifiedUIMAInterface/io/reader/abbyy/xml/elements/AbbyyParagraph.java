package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.assertj.core.util.Strings;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.annotation.ocr.abbyy.Paragraph;
import org.xml.sax.Attributes;

public class AbbyyParagraph extends AbstractAnnotation {
    private enum Alignment {
        Left, Center, Right, Justified
    }

    private final int leftIndent;
    private final int rightIndent;
    private final int startIndent;
    private final int lineSpacing;
    private final Alignment alignment;

    public AbbyyParagraph(Attributes attributes) {
        this.leftIndent = Utils.parseInt(attributes.getValue("leftIndent"));
        this.rightIndent = Utils.parseInt(attributes.getValue("rightIndent"));
        this.startIndent = Utils.parseInt(attributes.getValue("startIndent"));
        this.lineSpacing = Utils.parseInt(attributes.getValue("lineSpacing"));

        Alignment alignment = Alignment.Left;
        try {
            String align = attributes.getValue("align");
            if (!Strings.isNullOrEmpty(align)) {
                alignment = Alignment.valueOf(align);
            }
        } catch (IllegalArgumentException ignore) {
        }
        this.alignment = alignment;
    }

    @Override
    public Paragraph into(JCas jcas, int start, int end) {
        Paragraph paragraph = new Paragraph(jcas, start, end);
        paragraph.setLeftIndent(leftIndent);
        paragraph.setRightIndent(rightIndent);
        paragraph.setStartIndent(startIndent);
        paragraph.setLineSpacing(lineSpacing);
        paragraph.setAlignment(alignment.name());
        return paragraph;
    }
}
