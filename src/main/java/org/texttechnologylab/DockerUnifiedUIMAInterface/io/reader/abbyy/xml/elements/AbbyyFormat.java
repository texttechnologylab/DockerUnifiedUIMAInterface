package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.xml.sax.Attributes;
import org.texttechnologylab.annotation.ocr.abbyy.Format;

public class AbbyyFormat extends AbstractAnnotation {
    private final String lang;
    private final String ff;
    private final float fs;
    private final boolean bold;
    private final boolean italic;
    private final boolean subscript;
    private final boolean superscript;
    private final boolean smallcaps;
    private final boolean underline;
    private final boolean strikeout;

    public AbbyyFormat(Attributes attributes) {
        this.lang = attributes.getValue("lang");
        this.ff = attributes.getValue("ff");
        this.fs = Utils.parseFloat(attributes.getValue("fs"));
        this.bold = Utils.parseBoolean(attributes.getValue("bold"));
        this.italic = Utils.parseBoolean(attributes.getValue("italic"));
        this.subscript = Utils.parseBoolean(attributes.getValue("subscript"));
        this.superscript = Utils.parseBoolean(attributes.getValue("superscript"));
        this.smallcaps = Utils.parseBoolean(attributes.getValue("smallcaps"));
        this.underline = Utils.parseBoolean(attributes.getValue("underline"));
        this.strikeout = Utils.parseBoolean(attributes.getValue("strikeout"));
    }

    @Override
    public Format into(JCas jCas, int start, int end) {
        Format format = new Format(jCas, start, end);
        format.setLang(lang);
        format.setFf(ff);
        format.setFs(fs);
        format.setBold(bold);
        format.setItalic(italic);
        format.setSubscript(subscript);
        format.setSuperscript(superscript);
        format.setSmallcaps(smallcaps);
        format.setUnderline(underline);
        format.setStrikeout(strikeout);
        return format;
    }
}
