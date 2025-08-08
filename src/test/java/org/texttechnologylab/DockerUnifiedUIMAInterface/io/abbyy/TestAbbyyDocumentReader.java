package org.texttechnologylab.DockerUnifiedUIMAInterface.io.abbyy;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.AbbyyDocumentReader;

import java.io.IOException;
import java.nio.file.Paths;

public class TestAbbyyDocumentReader {

    @Test
    public void testReader() {
        String path = Paths.get("src/test/resources/abbyy/").toAbsolutePath().toString();

        try {
            CollectionReader reader = CollectionReaderFactory.createReader(AbbyyDocumentReader.class,
                    AbbyyDocumentReader.PARAM_SOURCE_LOCATION, path + "/xml/",
                    AbbyyDocumentReader.PARAM_ROOT_PATTERNS, "[+]10773178/",
                    AbbyyDocumentReader.PARAM_FILE_PATTERNS, "[+]*.xml.gz",
                    AbbyyDocumentReader.PARAM_DOCUMENT_ID_PATTERN, ".*/(\\d+)/?|(\\d+)[^/]*",
                    //
                    AbbyyDocumentReader.PARAM_METADATA_FILE, path + "/metadata.json",
                    // URI for UB Biodiversity corpus
                    AbbyyDocumentReader.PARAM_BASE_URI, "https://sammlungen.ub.uni-frankfurt.de/biodiv/",
                    // relative "inner" bounding box, inset to 1% of each pages' dimensions
                    AbbyyDocumentReader.PARAM_BOUNDING_BOX_DEF, "1",
                    // omit line format annotations
                    AbbyyDocumentReader.PARAM_ADD_LINE_FORMAT, false
            );
            AnalysisEngineDescription writer = AnalysisEngineFactory.createEngineDescription(
                    XmiWriter.class,
                    XmiWriter.PARAM_TARGET_LOCATION, "/tmp/duui/io/abbyy/",
                    XmiWriter.PARAM_PRETTY_PRINT, true,
                    XmiWriter.PARAM_OVERWRITE, true,
                    XmiWriter.PARAM_SANITIZE_ILLEGAL_CHARACTERS, true
            );
            SimplePipeline.runPipeline(reader, writer);
        } catch (UIMAException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
