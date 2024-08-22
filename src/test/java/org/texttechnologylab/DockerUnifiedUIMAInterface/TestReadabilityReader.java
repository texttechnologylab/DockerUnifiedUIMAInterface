package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XMLSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability.DUUIHTMLReadabilityReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability.HTMLReadabilityLoader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByAnnotation;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
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
    public void testSRLC07() throws Exception {    
	Path sourceLocation = Paths.get("/storage/projects/CORE/projects2/B05_C07_Corpus/texts_xmi_1_spacy/");
	Path targetLocation = Paths.get("/storage/projects/CORE/projects2/B05_C07_Corpus/texts_xmi_2_srl_ht/");
	int scale = 10;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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

	DUUIPipelineComponent componentLang = new DUUISwarmDriver
		.Component("docker.texttechnologylab.org/srl_cuda_1024:latest")
		.withScale(scale)
		.withConstraintHost("isengart")
		.build();
	composer.add(componentLang);
	
       	DUUIPipelineComponent componentSpacy = new DUUISwarmDriver
		.Component("docker.texttechnologylab.org/heideltime_ext:0.3")
                .withScale(scale)
		.withConstraintHost("isengart")
		.build();
	composer.add(componentSpacy);
	
	composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
		XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
		XmiWriter.PARAM_PRETTY_PRINT, true,
		XmiWriter.PARAM_OVERWRITE, true,
		XmiWriter.PARAM_VERSION, "1.1",
		XmiWriter.PARAM_COMPRESSION, "GZIP"
	)).build());

	composer.run(processor, "srl_ht");
	composer.shutdown();
    }

    
    @Test
    public void testSpacy() throws Exception {    
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
	int scale = 50;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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

	DUUIPipelineComponent componentLang = new DUUISwarmDriver
		.Component("docker.texttechnologylab.org/languagedetection:0.5")
		.withScale(scale)
		.withConstraintHost("isengart")
		.build();
	composer.add(componentLang);

	DUUISegmentationStrategyByAnnotation strategy = new DUUISegmentationStrategyByAnnotation()
		.withSegmentationClass(Paragraph.class)
		.withMaxAnnotationsPerSegment(1)
		.withMaxCharsPerSegment(1000000)
		.withPrintStatistics(false);
	
       	DUUIPipelineComponent componentSpacy = new DUUISwarmDriver.Component("docker.texttechnologylab.org/duui-spacy:0.4.3")
        //DUUIPipelineComponent componentSpacy = new DUUISwarmDriver.Component("docker.texttechnologylab.org/duui-spacy-de_core_news_lg:0.4.1")
                .withScale(scale)
		.withConstraintHost("isengart")
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
    public void testReaderAlle() throws ParserConfigurationException, IOException, UIMAException, SAXException {
	Path listFile = Paths.get("/storage/projects/CORE/erhebungen/t0/db/texts_t0_2024_07_29.csv");
	System.out.println(listFile);
	try (BufferedReader reader =  Files.newBufferedReader(listFile, StandardCharsets.UTF_8)) {
	    long countNew = 0;
	    long countExists = 0;
	    long counter = 0;
	    boolean skipFirstLine = true;
	    String line;
	    while ((line = reader.readLine()) != null) {
		try {
			counter += 1;
			if (counter % 1000 == 0) {
			    System.out.println("C07: " + counter);
			}

			if (skipFirstLine) {
			    skipFirstLine = false;
			    continue;
			}

			line = line.trim();
			String[] fields = line.split("\t", -1);

			String user = fields[12];
			if (user.startsWith("\"")) {
				user = user.substring(1, user.length()-1);
			}
			String session = fields[9];
			String html = fields[14];

			String title = html + ".html.gz";
			String docId = user + "/" + session + "/" + title;
			String collectionId = "file:/storage/projects/CORE/azure/core-edutec-fileshare/texts/";
			String docBaseUri = collectionId;
			String docUri = docBaseUri + docId;

			Path filename = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts/" + docId);
			Path output = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi/" + docId + ".xmi.gz");
			System.out.println(output);
			if (Files.exists(output)) {
				System.out.println("exists: " + output.toString());
				countExists++;
				continue;
			}

			JCas jCas = HTMLReadabilityLoader.load(filename, null);
			
			DocumentMetaData dmd = new DocumentMetaData(jCas);
			dmd.setDocumentTitle(title);
			dmd.setDocumentId(docId);
			dmd.setDocumentUri(docUri);
			dmd.setCollectionId(collectionId);
			dmd.setDocumentBaseUri(docBaseUri);
			dmd.addToIndexes();

			Files.createDirectories(output.getParent());
			try(GZIPOutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(output))) {
			    XMLSerializer xmlSerializer = new XMLSerializer(outputStream, true);
			    xmlSerializer.setOutputProperty(OutputKeys.VERSION, "1.1");
			    xmlSerializer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.toString());
			    XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
			    xmiCasSerializer.serialize(jCas.getCas(), xmlSerializer.getContentHandler());
			}
			
			countNew++;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	    }

		System.out.println("Count    " + counter);
		System.out.println("    New: " + countNew);
		System.out.println(" Exists: " + countExists);
	}
    }
    @Test
    public void testExportC07() throws ParserConfigurationException, IOException, UIMAException, SAXException {
	Path listFile = Paths.get("/storage/projects/CORE/erhebungen/t0/db/assessment_urls_texts_4_c07.csv");
	System.out.println(listFile);
	try (BufferedReader reader =  Files.newBufferedReader(listFile, StandardCharsets.UTF_8)) {
	    long countOk = 0;
	    long countError = 0;
	    long counter = 0;
	    boolean skipFirstLine = true;
	    String line;
	    while ((line = reader.readLine()) != null) {
		try {
			if (skipFirstLine) {
			    skipFirstLine = false;
			    continue;
			}

			counter += 1;
			if (counter % 1000 == 0) {
			    System.out.println("C07: " + counter);
			}

			line = line.trim();
			String[] fields = line.split("\t", -1);

			String user = fields[11];
			String session = fields[6];
			String html = fields[12];

			String title = html + ".html.gz.xmi.gz";
			String docId = user + "/" + session + "/" + title;
			Path input = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy/" + docId);
			//System.out.println(input);

			if (Files.exists(input)) {
				countOk++;

				String docIdOut = user + "_" + session + "_" + title;
				Path output = Paths.get("/storage/projects/CORE/projects2/B05_C07_Corpus/texts_xmi_1_spacy/" + docIdOut);
				if (!Files.exists(output)) {
					//System.out.println(output);
					Files.copy(input, output, StandardCopyOption.COPY_ATTRIBUTES);
				}
			}
			else {
				countError++;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	    }

		System.out.println("Count : " + counter);
		System.out.println("    Ok: " + countOk);
		System.out.println(" Error: " + countError);
	}
    }

    @Test
    public void testReaderC07() throws ParserConfigurationException, IOException, UIMAException, SAXException {
	Path listFile = Paths.get("/storage/projects/CORE/erhebungen/t0/db/assessment_urls_texts_4_c07.csv");
	System.out.println(listFile);
	try (BufferedReader reader =  Files.newBufferedReader(listFile, StandardCharsets.UTF_8)) {
	    long countNew = 0;
	    long countExists = 0;
	    long counter = 0;
	    boolean skipFirstLine = true;
	    String line;
	    while ((line = reader.readLine()) != null) {
		try {
			counter += 1;
			if (counter % 1000 == 0) {
			    System.out.println("C07: " + counter);
			}

			if (skipFirstLine) {
			    skipFirstLine = false;
			    continue;
			}

			line = line.trim();
			String[] fields = line.split("\t", -1);

			String user = fields[11];
			String session = fields[6];
			String html = fields[12];

			String title = html + ".html.gz";
			String docId = user + "/" + session + "/" + title;
			String collectionId = "file:/storage/projects/CORE/azure/core-edutec-fileshare/texts/";
			String docBaseUri = collectionId;
			String docUri = docBaseUri + docId;

			Path filename = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts/" + docId);
			Path output = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi/" + docId + ".xmi.gz");
			System.out.println(output);
			if (Files.exists(output)) {
				System.out.println("exists: " + output.toString());
				countExists++;
				continue;
			}

			JCas jCas = HTMLReadabilityLoader.load(filename, null);
			
			DocumentMetaData dmd = new DocumentMetaData(jCas);
			dmd.setDocumentTitle(title);
			dmd.setDocumentId(docId);
			dmd.setDocumentUri(docUri);
			dmd.setCollectionId(collectionId);
			dmd.setDocumentBaseUri(docBaseUri);
			dmd.addToIndexes();

			Files.createDirectories(output.getParent());
			try(GZIPOutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(output))) {
			    XMLSerializer xmlSerializer = new XMLSerializer(outputStream, true);
			    xmlSerializer.setOutputProperty(OutputKeys.VERSION, "1.1");
			    xmlSerializer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.toString());
			    XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
			    xmiCasSerializer.serialize(jCas.getCas(), xmlSerializer.getContentHandler());
			}
			
			countNew++;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	    }

		System.out.println("Count    " + counter);
		System.out.println("    New: " + countNew);
		System.out.println(" Exists: " + countExists);
	}
    }

    @Test
    public void testReader2() throws ParserConfigurationException, IOException, UIMAException, SAXException {
	List<String> tasks = new ArrayList();
	tasks.add("Start-Up-Aufgabe");
	/*tasks.add("Medizin-Kreislauf");
	tasks.add("Medizin-Mittelohr");
	tasks.add("Piloten-Streik-Aufgabe");
	tasks.add("Startup-Aufgabe");
	tasks.add("Windpark-Aufgabe");
	tasks.add("Nudging-Aufgabe");
	tasks.add("Medizin-Atmung");
	tasks.add("Hitzestift");
	tasks.add("Gruene-Sosse");
	tasks.add("Tetra-Pak");*/

	for (String task : tasks) {
		Path listFile = Paths.get("/storage/projects/CORE/erhebungen/t0/db/tasks/assessment_urls_texts_" + task  + "_v2.csv");
		System.out.println(listFile);
		try (BufferedReader reader =  Files.newBufferedReader(listFile, StandardCharsets.UTF_8)) {
		    long countNew = 0;
		    long countExists = 0;
		    long counter = 0;
		    boolean skipFirstLine = true;
		    String line;
		    while ((line = reader.readLine()) != null) {
			try {
				counter += 1;
				if (counter % 1000 == 0) {
				    System.out.println(task + ": " + counter);
				}

				if (skipFirstLine) {
				    skipFirstLine = false;
				    continue;
				}

				line = line.trim();
				String[] fields = line.split("\t", -1);

				String user = fields[9];
				String session = fields[4];
				String html = fields[10];

				String title = html + ".html.gz";
				String docId = user + "/" + session + "/" + title;
				String collectionId = "file:/storage/projects/CORE/azure/core-edutec-fileshare/texts/";
				String docBaseUri = collectionId;
				String docUri = docBaseUri + docId;

				Path filename = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts/" + docId);
				Path output = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi/" + docId + ".xmi.gz");
				if (Files.exists(output)) {
					//System.out.println("exists: " + output.toString());
					countExists++;
					continue;
				}

				JCas jCas = HTMLReadabilityLoader.load(filename, null);
				
				DocumentMetaData dmd = new DocumentMetaData(jCas);
				dmd.setDocumentTitle(title);
				dmd.setDocumentId(docId);
				dmd.setDocumentUri(docUri);
				dmd.setCollectionId(collectionId);
				dmd.setDocumentBaseUri(docBaseUri);
				dmd.addToIndexes();

				Files.createDirectories(output.getParent());
				try(GZIPOutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(output))) {
				    XMLSerializer xmlSerializer = new XMLSerializer(outputStream, true);
				    xmlSerializer.setOutputProperty(OutputKeys.VERSION, "1.1");
				    xmlSerializer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.toString());
				    XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
				    xmiCasSerializer.serialize(jCas.getCas(), xmlSerializer.getContentHandler());
				}
				
				countNew++;
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		    }

			System.out.println("Count    " + counter);
			System.out.println("    New: " + countNew);
			System.out.println(" Exists: " + countExists);
		}
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


    @Test
    public void testDDC2() throws Exception {    
		// DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/topic_ddc2_100");
		int scale = 20;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String ddcVariant = "ddc2_dim100";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/textimager-duui-ddc-fasttext:latest")
					.withParameter("ddc_variant", ddcVariant)
					.withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }

    @Test
    public void testTopicCardiffnlp() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/topic_cardiffnlp");
		int scale = 2;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "cardiffnlp/tweet-topic-latest-multi";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-topic:latest")
                    .withParameter("model_name", model)
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }

    @Test
    public void testGenreClassla() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/genre_classla");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
                )
        );

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(scale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
		DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIKubernetesDriver kubernetesDriver = new DUUIKubernetesDriver();
        composer.addDriver(uimaDriver, swarmDriver, dockerDriver, kubernetesDriver);


		String model = "";
		model = "classla/xlm-roberta-base-multilingual-text-genre-classifier";
		//omposer.add(new DUUISwarmDriver.
		composer.add(new DUUIKubernetesDriver.
					Component("docker.texttechnologylab.org/duui-transformers-topic:latest")
                    .withParameter("model_name", model)
                    // .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    .withParameter("selection", "text")
					.withScale(scale)
					.withLabels("hostname=isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }

/*
    @Test
    public void testGenreClasslaSents() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/genre_classla_sents");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "";
		model = "classla/xlm-roberta-base-multilingual-text-genre-classifier";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-topic:latest")
                    .withParameter("model_name", model)
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    // .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }


	@Test
	public void testSentimentCardiffnlp() throws Exception {    
		// (NOT) DONE
		Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
		Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/sentiment_cardiffnlp");
		int scale = 1;

		DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
				new DUUIFileReader(
						sourceLocation.toString(),
						"html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "";
		model = "cardiffnlp/twitter-xlm-roberta-base-sentiment";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-sentiment:latest")
					.withParameter("model_name", model)
					.withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
					// .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
	}


    @Test
    public void testSentimentNLPTown() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/sentiment_nlptown");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "";
		model = "nlptown/bert-base-multilingual-uncased-sentiment";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-sentiment:latest")
                    .withParameter("model_name", model)
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    // .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }

    @Test
    public void testSentimentVader() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/sentiment_vader");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/textimager-duui-vader-sentiment:latest")
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    // .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }


    @Test
    public void testToxicCitizenlab() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/toxicity_citizenlab");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "";
		model = "citizenlab/distilbert-base-multilingual-cased-toxicity";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-toxic:latest")
                    .withParameter("model_name", model)
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    // .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }


    @Test
    public void testToxicEIS() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/toxicity_eis");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "";
		model = "EIStakovskii/xlm_roberta_base_multilingual_toxicity_classifier_plus";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-toxic:latest")
                    .withParameter("model_name", model)
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    // .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }


    @Test
    public void testToxicEIS() throws Exception {    
		// (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy");
        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/toxicity_FredZhang7");
		int scale = 1;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz",
			1,
			-1,
			false,
			"",
			false,
			null,
			-1,
			targetLocation.toString(),
			"html.gz.xmi.gz"
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


		String model = "";
		model = "FredZhang7/one-for-all-toxicity-v3";
		composer.add(new DUUISwarmDriver.
					Component("docker.texttechnologylab.org/duui-transformers-toxic:latest")
                    .withParameter("model_name", model)
                    .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
				    // .withParameter("selection", "text")
					.withScale(scale)
					.withConstraintHost("isengart")
					.build()
		);

		composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
			XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
			XmiWriter.PARAM_PRETTY_PRINT, true,
			XmiWriter.PARAM_OVERWRITE, true,
			XmiWriter.PARAM_VERSION, "1.1",
			XmiWriter.PARAM_COMPRESSION, "GZIP"
		)).build());

		composer.run(processor, "spacy_plus");
		composer.shutdown();
    }*/
}
