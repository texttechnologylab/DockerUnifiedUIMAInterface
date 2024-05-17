package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XMLSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability.DUUIHTMLReadabilityReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability.HTMLReadabilityLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

public class TestReadabilityReader {
    @Test
    public void testSimple() throws ParserConfigurationException, IOException, UIMAException, SAXException {
        String language = "de";
        Path filename = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts/88520b38-5f53-4752-95d1-c2acf7a6630d/7287/1042042.html.gz");
        JCas cas = HTMLReadabilityLoader.load(filename, language);

        try(GZIPOutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(Paths.get("test.xmi.gz")))) {
            XMLSerializer xmlSerializer = new XMLSerializer(outputStream, true);
            xmlSerializer.setOutputProperty(OutputKeys.VERSION, "1.1");
            xmlSerializer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.toString());
            XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
            xmiCasSerializer.serialize(cas.getCas(), xmlSerializer.getContentHandler());
        }

        for (Paragraph paragraph : cas.select(Paragraph.class)) {
            System.out.println(paragraph.getCoveredText());
        }
    }

    @Test
    public void testReader() throws Exception {
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi");

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        composer.addDriver(uimaDriver);

        CollectionReaderDescription reader = createReaderDescription(DUUIHTMLReadabilityReader.class
                , DUUIHTMLReadabilityReader.PARAM_SOURCE_LOCATION, sourceLocation.toString()
                , DUUIHTMLReadabilityReader.PARAM_PATTERNS, "[+]**/*.html.gz"
                , DUUIHTMLReadabilityReader.PARAM_LANGUAGE, "de"
        );

        composer.add(new DUUIUIMADriver.Component(
                createEngineDescription(XmiWriter.class
                        , XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString()
                        , XmiWriter.PARAM_PRETTY_PRINT, true
                        , XmiWriter.PARAM_OVERWRITE, true
                        , XmiWriter.PARAM_VERSION, "1.1"
                        , XmiWriter.PARAM_COMPRESSION, "GZIP"
                )
        ));

        composer.run(reader, "readability_html");
    }
}
