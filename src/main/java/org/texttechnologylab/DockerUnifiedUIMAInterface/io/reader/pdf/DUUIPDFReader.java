package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.pdf;

import org.apache.uima.UimaContext;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.bytes.DUUIBytesReader;

import java.io.IOException;
import java.nio.file.Path;

public class DUUIPDFReader extends DUUIBytesReader {
    protected static final String PDF_MIME_TYPE = "application/pdf";

    @Override
    public void initialize(UimaContext aContext) throws ResourceInitializationException {
        super.initialize(aContext);

        if (mimeType == null) {
            mimeType = PDF_MIME_TYPE;
        }
    }

    public static JCas load(Path path) throws IOException, ResourceInitializationException, CASException {
        return DUUIBytesReader.load(path, PDF_MIME_TYPE);
    }

    public static void load(JCas jCas, Path path) throws IOException, ResourceInitializationException, CASException {
        DUUIBytesReader.load(jCas, path, PDF_MIME_TYPE);
    }
}
