package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class DUUISegmentationBenchmark {
    static JCas createCas(JCas jCas, int sentencesCount) throws UIMAException {
        // create cas by duplicating existing text and sentences
        JCas jCasLarge = JCasFactory.createJCas();
        jCasLarge.setDocumentLanguage(jCas.getDocumentLanguage());

        StringBuilder sb = new StringBuilder();
        boolean finished = false;
        int sentencesCountCurrent = 0;
        while (!finished) {
            for (Sentence os : JCasUtil.select(jCas, Sentence.class)) {
                try {
                    int offset = sb.length();
                    String text = os.getCoveredText();
                    Sentence ns = new Sentence(jCasLarge, offset, text.length() + offset);
                    ns.addToIndexes();
                    sb.append(text).append(" ");
                    sentencesCountCurrent += 1;
                    if (sentencesCountCurrent >= sentencesCount) {
                        finished = true;
                        break;
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        jCasLarge.setDocumentText(sb.toString());
        System.out.println("after: " + JCasUtil.select(jCasLarge, Sentence.class).size());

        return jCasLarge;
    }

    static JCas duplicateCas(JCas jCas, int count) throws UIMAException {
        JCas jCasLarge = JCasFactory.createJCas();
        jCasLarge.setDocumentLanguage(jCas.getDocumentLanguage());

        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < count; i++) {
            for (Sentence os : JCasUtil.select(jCas, Sentence.class)) {
                try {
                    int offset = sb.length();
                    String text = os.getCoveredText();
                    Sentence ns = new Sentence(jCasLarge, offset, text.length() + offset);
                    ns.addToIndexes();
                    sb.append(text).append(" ");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        jCasLarge.setDocumentText(sb.toString());
        System.out.println("after: " + JCasUtil.select(jCasLarge, Sentence.class).size());

        return jCasLarge;
    }

    static public void benchmarkSpacyGerparcor() throws Exception {
        String small2mbFilename = "src/test/resources/org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation/11_002.xmi.gz.xmi.gz";
        String large16mbFilename = "src/test/resources/org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation/12_041.xmi.gz.xmi.gz";
        String filename = large16mbFilename;

        JCas jCas = JCasFactory.createJCas();
        CasIOUtils.load(
                new GZIPInputStream(Files.newInputStream(Paths.get(filename))),
                jCas.getCas()
        );

        JCas jCasBenchmark = duplicateCas(jCas, 8);

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        String url = "http://127.0.0.1:9714";
        DUUIRemoteDriver.Component component = new DUUIRemoteDriver.Component(url);

        // Disable integrated segmentation
        component.withParameter("split_large_texts", "false");

        component.withSegmentationStrategy(
                new DUUISegmentationStrategyByAnnotation()
                        .withSegmentationClass(Sentence.class)
                        .withMaxAnnotationsPerSegment(1000)
                        .withMaxCharsPerSegment(1000000)
        );

        composer.add(component);

        // run benchmark
        // TODO warmup
        long startTime = System.nanoTime();
        composer.run(jCasBenchmark);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;  // divide by 1000000 to get milliseconds.
        System.out.println("-> " + duration + "ms" + ", characters = " + jCasBenchmark.getDocumentText().length());

        composer.shutdown();
    }

    public static void main(String[] args) throws Exception {
        benchmarkSpacyGerparcor();

        /*
        // load base document with sentences
        String baseDocumentFilename = "src/test/resources/org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation/doc1.xmi.gz";
        JCas jCas = JCasFactory.createJCas();
        CasIOUtils.load(
                new GZIPInputStream(Files.newInputStream(Paths.get(baseDocumentFilename))),
                jCas.getCas()
        );

        String resultsCsvFilename = "segmentation_benchmark.csv";

        // benchmark different sized documents
        List<Boolean> segmentationStatuses = Arrays.asList(true, false);
        List<Integer> sentencesCounts = Arrays.asList(1, 5, 10, 100, 500, 1000, 1500, 2000, 2500, 5000, 10000);
        List<Integer> maxSentencesPerSegment = Arrays.asList(1, 5, 10, 100, 500, 1000);

        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(resultsCsvFilename), StandardCharsets.UTF_8)) {
            writer.write("sentences\tcharacters\tsegmentation\tmax_sentences\tduration_ms\n");

            for (int sentencesCount : sentencesCounts) {
                for (boolean segmentationStatus : segmentationStatuses) {
                    for (int maxSentences : maxSentencesPerSegment) {
                        System.out.println("Benchmarking " + sentencesCount + " sentences with segmentation: " + segmentationStatus);

                        // always recreate duui
                        DUUIComposer composer = new DUUIComposer()
                                .withSkipVerification(true)
                                .withLuaContext(new DUUILuaContext().withJsonLibrary());

                        // TODO switch to docker driver later
                        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
                        composer.addDriver(remoteDriver);

                        // Add component for benchmarking
                        String url = "http://127.0.0.1:9714";
                        DUUIRemoteDriver.Component component = new DUUIRemoteDriver.Component(url);
                        component.withParameter("split_large_texts", "false");

                        // configure component with segmentation strategy
                        // TODO allow different configs here
                        if (segmentationStatus) {
                            component.withSegmentationStrategy(
                                    new DUUISegmentationStrategyByAnnotation()
                                            .withSegmentationClass(Sentence.class)
                                            .withMaxAnnotationsPerSegment(maxSentences)
                                            .withMaxCharsPerSegment(10000)
                            );
                        }

                        composer.add(component);

                        // create larger cas for benchmarking
                        JCas jCasBenchmark = createCas(jCas, sentencesCount);

                        // run benchmark
                        // TODO warmup
                        long startTime = System.nanoTime();
                        composer.run(jCasBenchmark);
                        long endTime = System.nanoTime();
                        long duration = (endTime - startTime) / 1000000;  // divide by 1000000 to get milliseconds.
                        System.out.println("-> " + duration + "ms");
                        writer.write(sentencesCount + "\t" + jCasBenchmark.getDocumentText().length() + "\t" + segmentationStatus + "\t" + maxSentences + "\t" + duration + "\n");

                        composer.shutdown();
                    }
                }
            }
        }*/
    }
}
