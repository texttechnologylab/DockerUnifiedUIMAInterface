package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.Sofa;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.xml.sax.SAXException;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;

public class TestAnnotationDropper {
    static JCas jCas;
    static DUUIComposer composer;

    static final List<String[]> sentences = Arrays.asList(
            new String[] { "This", "is", "a", "sentence", "." },
            new String[] { "This", "is", "another", "sentence", "." },
            new String[] { "This", "is", "a", "third", "sentence", "." });

    @BeforeAll
    static void setUp() throws ResourceInitializationException {
        try {
            jCas = JCasFactory.createJCas();
        } catch (ResourceInitializationException | CASException e) {
            throw new ResourceInitializationException(e);
        }
        resetCas();

        Assertions.assertEquals(3, JCasUtil.select(jCas, Sentence.class).size());
        Assertions.assertEquals(16, JCasUtil.select(jCas, Token.class).size());

        try {
            composer = new DUUIComposer()
                    .withSkipVerification(true)
                    .withWorkers(1);
        } catch (URISyntaxException e) {
            throw new ResourceInitializationException(e);
        }

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver().withDebug(false);
        composer.addDriver(uimaDriver);
    }

    @AfterEach
    public void afterEach() throws IOException, SAXException {
        composer.resetPipeline();
        resetCas();
    }

    static void resetCas() {
        jCas.reset();
        jCas.setDocumentText(sentences.stream().flatMap(Arrays::stream).collect(Collectors.joining(" ")));
        int tokenOffset = 0;
        int sentenceOffset = 0;
        for (String[] sentence : sentences) {
            String text = String.join(" ", sentence);
            jCas.addFsToIndexes(new Sentence(jCas, sentenceOffset, sentenceOffset + text.length()));
            sentenceOffset += text.length() + 1;
            for (String token : sentence) {
                jCas.addFsToIndexes(new Token(jCas, tokenOffset, tokenOffset + token.length()));
                tokenOffset += token.length() + 1;
            }
        }
    }

    @AfterAll
    static void afterAll() throws IOException, InterruptedException {
        composer.shutdown();
    }

    @Test
    public void testTypesToRetain() throws ResourceInitializationException, CASException {
        try {
            AnalysisEngine dropper = createEngine(
                    AnnotationDropper.class,
                    AnnotationDropper.PARAM_TYPES_TO_RETAIN,
                    new String[] {
                            Sofa._TypeName,
                            org.apache.uima.jcas.tcas.DocumentAnnotation._TypeName,
                            org.texttechnologylab.annotation.DocumentAnnotation._TypeName,
                            Sentence._TypeName,
                    });

            try {
                dropper.process(jCas);
            } catch (AnalysisEngineProcessException e) {
                throw new RuntimeException(e);
            }

            Assertions.assertEquals(3, JCasUtil.select(jCas, Sentence.class).size());
            Assertions.assertEquals(0, JCasUtil.select(jCas, Token.class).size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTypesToDrop() throws ResourceInitializationException, CASException {
        try {
            AnalysisEngine dropper = createEngine(
                    AnnotationDropper.class,
                    AnnotationDropper.PARAM_TYPES_TO_DROP,
                    new String[] {
                            Token._TypeName,
                    });

            try {
                dropper.process(jCas);
            } catch (AnalysisEngineProcessException e) {
                throw new RuntimeException(e);
            }

            Assertions.assertEquals(3, JCasUtil.select(jCas, Sentence.class).size());
            Assertions.assertEquals(0, JCasUtil.select(jCas, Token.class).size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTypesToRetainDUUI() {
        try {
            composer.add(new DUUIUIMADriver.Component(createEngineDescription(
                    AnnotationDropper.class,
                    AnnotationDropper.PARAM_TYPES_TO_RETAIN,
                    new String[] {
                            Sofa._TypeName,
                            Sentence._TypeName,
                    })));

            try {
                composer.run(jCas);
            } catch (Exception e) {
                Assertions.fail("DUUIComposer failed", e);
            }

            Assertions.assertEquals(3, JCasUtil.select(jCas, Sentence.class).size());
            Assertions.assertEquals(0, JCasUtil.select(jCas, Token.class).size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTypesToDropDUUI() {
        try {
            composer.add(new DUUIUIMADriver.Component(createEngineDescription(
                    AnnotationDropper.class,
                    AnnotationDropper.PARAM_TYPES_TO_DROP,
                    new String[] {
                            Token._TypeName,
                    })));

            try {
                composer.run(jCas);
            } catch (Exception e) {
                Assertions.fail("DUUIComposer failed", e);
            }

            Assertions.assertEquals(3, JCasUtil.select(jCas, Sentence.class).size());
            Assertions.assertEquals(0, JCasUtil.select(jCas, Token.class).size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
