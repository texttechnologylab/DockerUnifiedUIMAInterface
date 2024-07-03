package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;

import java.io.IOException;
import java.io.InputStream;

public class DUUIDocumentDecoder {

    public static InputStream decode(DUUIDocument document) throws IOException {
        String fileExtension = document.getFileExtension();

        try {
            if (fileExtension.equalsIgnoreCase(CompressorStreamFactory.GZIP)) {
                return new CompressorStreamFactory()
                    .createCompressorInputStream(
                        CompressorStreamFactory.GZIP,
                        document.toInputStream()
                    );

            }

            if (fileExtension.equalsIgnoreCase(CompressorStreamFactory.XZ)) {
                return new CompressorStreamFactory()
                    .createCompressorInputStream(
                        CompressorStreamFactory.XZ,
                        document.toInputStream()
                    );
            }
        } catch (CompressorException e) {
            throw new IOException("Document is not in the correct format ." + fileExtension);
        }
        return document.toInputStream();
    }
}

