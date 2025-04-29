package org.texttechnologylab.duui.lua_process;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.annotation.SpacyAnnotatorMetaData;

public class TestSpacyLuaProcess {
    @Test
    public void test_with_sentences() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(
                        new DUUILuaContext().withJsonLibrary()
                )
                .withSkipVerification(true)
                .addDriver(new DUUIRemoteDriver())
                .addDriver(new DUUIDockerDriver());

        composer.add(
                new DUUIDockerDriver.Component(
                        "duui-spacy-v2:dev"
                )
                        .withParameter("spacy_model_size", "lg")
                        .build()
        );

        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(
                "Die Goethe Universität ist auf vier große Universitätsgelände über das Frankfurter Stadtgebiet verteilt.\n "
                        + "Barack Obama war der 44. Präsident der Vereinigten Staaten von Amerika."
        );
        jCas.setDocumentLanguage("de");
        new Sentence(jCas, 0, 104).addToIndexes();
        new Sentence(jCas, 106, 177).addToIndexes();

        composer.run(jCas, "lua-process-test");

        printResult(jCas);
    }

    @Test
    public void test_wo_sentences() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(
                        new DUUILuaContext().withJsonLibrary()
                )
                .withSkipVerification(true)
                .addDriver(new DUUIRemoteDriver())
                .addDriver(new DUUIDockerDriver());

        composer.add(
                new DUUIDockerDriver.Component(
                        "duui-spacy-v2:dev"
                )
                        .withName("duui-spacy-v2:dev")
                        .withParameter("spacy_model_size", "sm")
                        .build()
        );

        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(
                "Die Goethe Universität ist auf vier große Universitätsgelände über das Frankfurter Stadtgebiet verteilt.\n "
                        + "Barack Obama war der 44. Präsident der Vereinigten Staaten von Amerika."
        );
        jCas.setDocumentLanguage("de");

        composer.run(jCas, "lua-process-test");

        printResult(jCas);
    }

    private static void printResult(JCas jCas) {
        System.out.println("### SpacyAnnotatorMetaData ###");
        for (SpacyAnnotatorMetaData annotation : JCasUtil.select(jCas, SpacyAnnotatorMetaData.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }

        System.out.println("### Sentence ###");
        for (Sentence annotation : JCasUtil.select(jCas, Sentence.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }

        System.out.println("### Token ###");
        for (Token annotation : JCasUtil.select(jCas, Token.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }

        System.out.println("### Lemma ###");
        for (Lemma annotation : JCasUtil.select(jCas, Lemma.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }

        System.out.println("### POS ###");
        for (POS annotation : JCasUtil.select(jCas, POS.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }

        System.out.println("### MorphologicalFeatures ###");
        for (MorphologicalFeatures annotation : JCasUtil.select(jCas, MorphologicalFeatures.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }

        System.out.println("### NamedEntity ###");
        for (NamedEntity annotation : JCasUtil.select(jCas, NamedEntity.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            sb.append("%n  text: '%s'".formatted(annotation.getCoveredText()));
            System.out.println(sb);
            System.out.println();
        }
    }
}
