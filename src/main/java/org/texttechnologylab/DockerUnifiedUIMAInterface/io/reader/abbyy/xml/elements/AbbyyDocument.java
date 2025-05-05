package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import com.google.common.base.Strings;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.annotation.ocr.abbyy.Document;
import org.xml.sax.Attributes;

public class AbbyyDocument extends AbstractAnnotation {

    // Mandatory
    private final String version;
    private final String producer;

    // Optional
    private final Integer pagesCount;
    private final String mainLanguage;
    private final String languages;

    // Additional
    public String documentName;

    public AbbyyDocument(Attributes attributes) {
        this.version = attributes.getValue("version");
        this.producer = attributes.getValue("producer");
        this.pagesCount = Utils.parseIntOr(attributes.getValue("pagesCount"), 1);
        this.mainLanguage = Strings.nullToEmpty(attributes.getValue("mainLanguage"));
        this.languages = Strings.nullToEmpty(attributes.getValue("languages"));
    }

    public void setDocumentName(String documentName) {
        this.documentName = documentName;
    }

    @Override
    public Document into(JCas jcas, int start, int end) {
        Document document = new Document(jcas, start, end);
        document.setVersion(version);
        document.setProducer(producer);
        document.setPagesCount(pagesCount);
        document.setMainLanguage(mainLanguage);
        document.setLanguages(languages);
        if (documentName != null) {
            document.setDocumentName(documentName);
        }
        return document;
    }
}
