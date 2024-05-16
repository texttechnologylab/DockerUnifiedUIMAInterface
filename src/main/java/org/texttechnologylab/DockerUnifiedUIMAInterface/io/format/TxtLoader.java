package org.texttechnologylab.DockerUnifiedUIMAInterface.io.format;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.io.IOUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class TxtLoader implements IDUUIFormat {
    private final String language;

    public TxtLoader() {
        this.language = null;
    }

    public TxtLoader(String language) {
        this.language = language;
    }

    @Override
    public void load(InputStream stream, JCas jCas) throws UIMAException, IOException {
        jCas.setDocumentText(IOUtils.toString(stream, StandardCharsets.UTF_8));

        if (language != null) {
            jCas.setDocumentLanguage(language);
        }

        // TODO meta
        DocumentMetaData meta = DocumentMetaData.create(jCas);
        meta.setCollectionId("");
        meta.setDocumentBaseUri("");
        meta.setDocumentUri("");
        meta.setDocumentId("");
        meta.setDocumentTitle("");
    }
}
