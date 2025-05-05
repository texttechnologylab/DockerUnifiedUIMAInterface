package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.annotation.ocr.abbyy.Page;
import org.xml.sax.Attributes;

public class AbbyyPage extends AbstractAnnotation {
    private final Integer width;
    private final Integer height;
    private final Integer resolution;
    private Orientation rotation = Orientation.Normal;
    //private final boolean originalCoords;

    public String pageId;
    public String pageUri;
    public Integer pageIndex;
    public String pageNumber;

    public AbbyyPage(Attributes attributes) {
        this.width = Utils.parseInt(attributes.getValue("width"));
        this.height = Utils.parseInt(attributes.getValue("height"));
        this.resolution = Utils.parseInt(attributes.getValue("resolution"));
        if (attributes.getValue("rotation") != null) {
            this.rotation = Orientation.valueOf(attributes.getValue("rotation"));
        }
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
        if (pageIndex != null) {
            page.setIndex(pageIndex);
        }
        if (pageNumber != null) {
            page.setPageNumber(pageNumber);
        }
        page.setHeight(height);
        page.setWidth(width);
        page.setResolution(resolution);
        page.setRotation(rotation.name());
        return page;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }


    public void setPageUri(String pageUri) {
        this.pageUri = pageUri;
    }

    public void setPageIndex(Integer pageIndex) {
        this.pageIndex = pageIndex;
    }

    public void setPageNumber(String pageNumber) {
        this.pageNumber = pageNumber;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }
}
