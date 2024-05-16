package org.texttechnologylab.DockerUnifiedUIMAInterface.io.format;

import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

public class XmiLoader implements IDUUIFormat {
    private final boolean lenient;

    public XmiLoader(boolean lenient) {
        this.lenient = lenient;
    }

    @Override
    public void load(InputStream stream, JCas jCas) throws UIMAException {
        try {
            XmiCasDeserializer.deserialize(stream, jCas.getCas(), lenient);
        } catch (SAXException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
