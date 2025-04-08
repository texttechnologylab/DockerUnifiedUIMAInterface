package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.annotation.ocr.abbyy.Page;
import org.xml.sax.Attributes;

public class AbbyyPage extends AbstractAnnotation {
    private final Integer width;
    private final Integer height;
    private final Integer resolution;
    //private final boolean originalCoords;

    public String pageUri;
    public Integer pageId;
    public String pageNumber;

    public AbbyyPage(Attributes attributes) {
        this.width = Utils.parseInt(attributes.getValue("width"));
        this.height = Utils.parseInt(attributes.getValue("height"));
        this.resolution = Utils.parseInt(attributes.getValue("resolution"));
        //this.originalCoords = TypeParser.parseBoolean(attributes.getValue("originalCoords"));
    }

    @Override
    public Page into(JCas jcas, int start, int end) {
        Page page = new Page(jcas, start, end);
        if (pageId != null) {
            page.setId(pageId);
        }
        if (pageUri != null) {
            page.setUri(pageUri);
        }
        if (pageNumber != null) {
            page.setNumber(pageNumber);
        }
        page.setHeight(height);
        page.setWidth(width);
        page.setResolution(resolution);
        return page;
    }

    public void setPageUri(String pageUri) {
        this.pageUri = pageUri;
    }

    public void setPageId(Integer pageId) {
        this.pageId = pageId;
    }

    public void setPageNumber(String pageNumber) {
        this.pageNumber = pageNumber;
    }
}
