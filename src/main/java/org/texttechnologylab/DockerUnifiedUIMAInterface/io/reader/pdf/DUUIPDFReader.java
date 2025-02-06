package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.pdf;

import org.apache.uima.UimaContext;
import org.apache.uima.resource.ResourceInitializationException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.bytes.DUUIBytesReader;

public class DUUIPDFReader extends DUUIBytesReader {
    @Override
    public void initialize(UimaContext aContext) throws ResourceInitializationException {
        super.initialize(aContext);

        if (mimeType == null) {
            mimeType = "application/pdf";
        }
    }
}
