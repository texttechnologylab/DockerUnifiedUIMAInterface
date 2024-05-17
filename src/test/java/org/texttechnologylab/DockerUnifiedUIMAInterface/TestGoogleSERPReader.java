package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XMLSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.google.DUUIHTMLGoogleSERPReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.google.HTMLGoogleSERPLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotation;
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

public class TestGoogleSERPReader {
    @Test
    public void testSimple() throws ParserConfigurationException, IOException, UIMAException, SAXException {
        String language = "de";
        Path filename = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/html/76a1aac7-017e-449a-9dc8-39acb5bf9490/7352/2334815.html.gz");
        JCas cas = HTMLGoogleSERPLoader.load(filename, language);

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
    public void testSpacy() throws Exception {
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/html_xmi_google_serps");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/html_xmi_google_serps_spacy");
        int scale = 10;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "xmi.gz"
                )
        );

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(scale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        composer.addDriver(uimaDriver, swarmDriver, dockerDriver);

        DUUISegmentationStrategyByAnnotation strategy = new DUUISegmentationStrategyByAnnotation()
                .withSegmentationClass(Paragraph.class)
                .withMaxAnnotationsPerSegment(1)
                .withMaxCharsPerSegment(1000000);

        DUUIPipelineComponent componentSpacy = new DUUISwarmDriver.Component("docker.texttechnologylab.org/duui-spacy-de_core_news_lg:0.4.1")
                .withScale(scale)
                .build();
        componentSpacy.withSegmentationStrategy(strategy);
        composer.add(componentSpacy);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(processor, "spacy");
        composer.shutdown();
    }

    @Test
    public void testReader() throws Exception {
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/html");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/html_xmi_google_serps");

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        composer.addDriver(uimaDriver);

        CollectionReaderDescription reader = createReaderDescription(DUUIHTMLGoogleSERPReader.class
                , DUUIHTMLGoogleSERPReader.PARAM_SOURCE_LOCATION, sourceLocation.toString()
                , DUUIHTMLGoogleSERPReader.PARAM_PATTERNS, "[+]**/*.html.gz"
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
