package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.AnnotationBase;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasCopier;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XMLSerializer;
import org.apache.uima.util.XmlCasSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.annotation.SpacyAnnotatorMetaData;
import org.xml.sax.SAXException;

import javax.xml.transform.OutputKeys;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DUUISegmentationTest {
    static DUUIComposer composer;
    static JCas jCas;

    static String url = "http://127.0.0.1:9714";

    @BeforeAll
    static void beforeAll() throws URISyntaxException, IOException, UIMAException {
        composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        jCas = JCasFactory.createJCas();
    }

    @AfterAll
    static void afterAll() throws UnknownHostException {
        composer.shutdown();
    }

    @AfterEach
    public void afterEach() throws IOException, SAXException {
        composer.resetPipeline();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jCas.getCas(), null, stream);
        System.out.println(stream.toString(StandardCharsets.UTF_8));

        jCas.reset();
    }

    @Test
    public void simpleSentenceTest() throws Exception {
        // given a large document, the text should be send in configurable segments
        // we require a document that has already ben tokenized and sentizized
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        composer.add(
                new DUUIRemoteDriver
                        .Component(url)
                        //.withSegmentationStrategy(DUUISegmentationStrategyBySentence.class)
                        .withSegmentationStrategy(
                                new DUUISegmentationStrategyByAnnotation()
                                        .withSegmentationClass(Sentence.class)
                                        .withMaxAnnotationsPerSegment(2)
                                        .withMaxCharsPerSegment(100)
                        )
        );

        Path inputFile = Paths.get("src/test/resources/org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation/doc1.xmi.gz");
        CasIOUtils.load(new GZIPInputStream(Files.newInputStream(inputFile)), jCas.getCas());

        List<Class<? extends TOP>> annotationsToCheck = new ArrayList<>();
        annotationsToCheck.add(Sentence.class);
        annotationsToCheck.add(Token.class);
        annotationsToCheck.add(Lemma.class);
        annotationsToCheck.add(POS.class);
        annotationsToCheck.add(NamedEntity.class);
        annotationsToCheck.add(SpacyAnnotatorMetaData.class);

        Map<Class<? extends TOP>, Integer> annotationsBefore = new HashMap<>();
        for (Class<? extends TOP> clazz : annotationsToCheck) {
            Collection<? extends TOP> annotations = JCasUtil.select(jCas, clazz);
            System.out.println(clazz.getName() + ": " + annotations.size());
            annotationsBefore.put(clazz, annotations.size());
        }

        System.out.println("sentences before");
        JCasUtil.select(jCas, Sentence.class).forEach(s -> System.out.println("++"+s.getCoveredText()+"++"));

        composer.run(jCas);

        System.out.println("sentences after");
        JCasUtil.select(jCas, Sentence.class).forEach(s -> System.out.println("++"+s.getCoveredText()+"++"));

        try(GZIPOutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(Paths.get("src/test/resources/org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation/doc1_out.xmi.gz")))) {
            XMLSerializer xmlSerializer = new XMLSerializer(outputStream, true);
            xmlSerializer.setOutputProperty(OutputKeys.VERSION, "1.1");
            xmlSerializer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.toString());
            XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
            xmiCasSerializer.serialize(jCas.getCas(), xmlSerializer.getContentHandler());
        }

        Map<Class<? extends TOP>, Integer> annotationsAfter = new HashMap<>();
        for (Class<? extends TOP> clazz : annotationsToCheck) {
            Collection<? extends TOP> annotations = JCasUtil.select(jCas, clazz);
            System.out.println(clazz.getName() + ": " + annotations.size());
            annotationsAfter.put(clazz, annotations.size());
        }

        // assert "before*2" and "after" are the same
        for (Class<? extends TOP> clazz : annotationsToCheck) {
            int beforeSize = annotationsBefore.get(clazz);
            int expectedSize = beforeSize*2;
            int afterSize = annotationsAfter.get(clazz);
            System.out.println(clazz.getName() + ": before=" + beforeSize + ", expected=" + expectedSize + ", after=" + afterSize);
            assertEquals(expectedSize, afterSize);
        }

        // TODO check exact begin/ends
        // TODO check contents on lemma, ne...

        // TODO referenzen werden aufgelöst bei "token", aber dann nicht bei "meta"?
    }

    @Test
    public void manualTest() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        composer.add(
                new DUUIRemoteDriver
                        .Component(url)
                        .withSegmentationStrategy(DUUISegmentationStrategyBySentence.class)
        );

        jCas = JCasFactory.createText(
                "Das ist der 1. Testsatz. Und hier ist der zweite. Zuletzt folgt der dritte - und letzte.",
                "de"
        );

        addSentence(0, 24);
        addSentence(25, 49);
        addSentence(50, 88);

        System.out.println("sentences before");
        JCasUtil.select(jCas, Sentence.class).forEach(s -> System.out.println("++"+s.getCoveredText()+"++"));

        composer.run(jCas);

        System.out.println("sentences after");
        JCasUtil.select(jCas, Sentence.class).forEach(s -> System.out.println("++"+s.getCoveredText()+"++"));
    }

    private Sentence addSentence(int begin, int end) {
        Sentence sentence = new Sentence(jCas, begin, end);
        sentence.addToIndexes();
        return sentence;
    }

    private Token addToken(int begin, int end) {
        Token token = new Token(jCas, begin, end);
        token.addToIndexes();

        AnnotationComment comment = new AnnotationComment(jCas);
        comment.setReference(token);
        comment.setKey("token_ref");
        comment.setValue(String.valueOf(token.getAddress()));
        comment.addToIndexes();

        return token;
    }

    private Lemma addLemma(Token token, String lemmaValue) {
        Lemma lemma = new Lemma(jCas, token.getBegin(), token.getEnd());
        lemma.setValue(lemmaValue);
        lemma.addToIndexes();
        token.setLemma(lemma);
        return lemma;
    }

    @Test
    public void casCopierIdsTest() throws UIMAException {
        // test handling of ids using the cas copier
        jCas = JCasFactory.createText("Das ist ein Test.", "de");
        addSentence(0, 17);
        addLemma(addToken(0, 3), "Das");
        addLemma(addToken(4, 7), "sein");
        addLemma(addToken(8, 11), "ein");
        addLemma(addToken(12, 16), "Text");
        addLemma(addToken(16, 17), ".");

        System.out.println("Original sentences:");
        for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
            System.out.println(sentence.getAddress() + " " + sentence.getCoveredText() + " ");
        }

        System.out.println("Original tokens:");
        for (Token token : JCasUtil.select(jCas, Token.class)) {
            System.out.println(token.getAddress() + " " + token.getCoveredText() + " " + " " + token.getLemma().getAddress());
        }

        System.out.println("Original lemmas:");
        for (Lemma lemma : JCasUtil.select(jCas, Lemma.class)) {
            System.out.println(lemma.getAddress() + "  " + lemma.getCoveredText() + " " + " " + lemma.getValue());
        }

        System.out.println("Original token comments:");
        for (AnnotationComment comment : JCasUtil.select(jCas, AnnotationComment.class)) {
            System.out.println(comment.getAddress() + " " + comment.getKey() + " " + " " + comment.getValue() + " " + comment.getReference().getAddress());
        }

        // create new cas and copy all tokens and sentences
        JCas jCasCopy = JCasFactory.createJCas();
        CasCopier copier = new CasCopier(jCas.getCas(), jCasCopy.getCas());

        for (AnnotationComment comment : JCasUtil.select(jCas, AnnotationComment.class)) {
            AnnotationBase copy = (AnnotationBase) copier.copyFs(comment);
            copy.addToIndexes(jCasCopy);
        }

        for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
            Annotation copy = (Annotation) copier.copyFs(sentence);
            copy.addToIndexes(jCasCopy);
        }

        for (Token token : JCasUtil.select(jCas, Token.class)) {
            Annotation copy = (Annotation) copier.copyFs(token);
            copy.addToIndexes(jCasCopy);
        }

        for (Lemma lemma : JCasUtil.select(jCas, Lemma.class)) {
            Annotation copy = (Annotation) copier.copyFs(lemma);
            copy.addToIndexes(jCasCopy);
        }

        jCasCopy.setDocumentText(jCas.getDocumentText());
        jCasCopy.setDocumentLanguage(jCas.getDocumentLanguage());

        System.out.println("Copied sentences:");
        for (Sentence sentence : JCasUtil.select(jCasCopy, Sentence.class)) {
            System.out.println(sentence.getAddress() + " " + sentence.getCoveredText() + " ");
        }

        System.out.println("Copied tokens:");
        for (Token token : JCasUtil.select(jCasCopy, Token.class)) {
            System.out.println(token.getAddress() + " " + token.getCoveredText() + " " + " " + token.getLemma().getAddress());
        }

        System.out.println("Copied lemmas:");
        for (Lemma lemma : JCasUtil.select(jCasCopy, Lemma.class)) {
            System.out.println(lemma.getAddress() + " " + lemma.getCoveredText() + " " + " " + lemma.getValue());
        }

        System.out.println("Copied token comments:");
        for (AnnotationComment comment : JCasUtil.select(jCasCopy, AnnotationComment.class)) {
            System.out.println(comment.getAddress() + " " + comment.getKey() + " " + " " + comment.getValue() + " " + comment.getReference().getAddress());
        }

        // conclusion:
        // the cas copier does not respect the ids and creates new ones
        // however, the references are correctly mapped, even if the lemmas are copied after the tokens
        // -> check segmentation why it does not work there

        // now copy back from new cas to original cas
        copier = new CasCopier(jCasCopy.getCas(), jCas.getCas());

        for (Sentence sentence : JCasUtil.select(jCasCopy, Sentence.class)) {
            Annotation copy = (Annotation) copier.copyFs(sentence);
            copy.addToIndexes(jCas);
        }

        for (Token token : JCasUtil.select(jCasCopy, Token.class)) {
            Annotation copy = (Annotation) copier.copyFs(token);
            copy.addToIndexes(jCas);
        }

        for (Lemma lemma : JCasUtil.select(jCasCopy, Lemma.class)) {
            Annotation copy = (Annotation) copier.copyFs(lemma);
            copy.addToIndexes(jCas);
        }

        for (AnnotationComment comment : JCasUtil.select(jCasCopy, AnnotationComment.class)) {
            AnnotationBase copy = (AnnotationBase) copier.copyFs(comment);
            copy.addToIndexes(jCas);
        }

        System.out.println("Back copied sentences:");
        for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
            System.out.println(sentence.getAddress() + " " + sentence.getCoveredText() + " ");
        }

        System.out.println("Back copied tokens:");
        for (Token token : JCasUtil.select(jCas, Token.class)) {
            System.out.println(token.getAddress() + " " + token.getCoveredText() + " " + " " + token.getLemma().getAddress());
        }

        System.out.println("Back copied lemmas:");
        for (Lemma lemma : JCasUtil.select(jCas, Lemma.class)) {
            System.out.println(lemma.getAddress() + " " + lemma.getCoveredText() + " " + " " + lemma.getValue());
        }

        System.out.println("Back copied token comments:");
        for (AnnotationComment comment : JCasUtil.select(jCas, AnnotationComment.class)) {
            System.out.println(comment.getAddress() + " " + comment.getKey() + " " + " " + comment.getValue() + " " + comment.getReference().getAddress());
        }

        // conclusion:
        // the cas copier duplicates the annotations
        // -> we cant just copy the results back to the original cas after processing
    }
}
