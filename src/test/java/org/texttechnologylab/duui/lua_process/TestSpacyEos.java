package org.texttechnologylab.duui.lua_process;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.junit.jupiter.api.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSpacyEos {
    public DUUIComposer composer = null;

    @BeforeAll
    public void init() throws Exception {
        composer = new DUUIComposer()
                .withLuaContext(
                        new DUUILuaContext().withJsonLibrary()
                )
                .withSkipVerification(true)
                .addDriver(new DUUIDockerDriver());
        composer.add(
                new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-spacy-eos:0.1.0")
                        .build()
        );
    }

    @Test
    public void testEnglish() throws Exception {
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(
                "Barack Hussein Obama II (born August 4, 1961) is an American politician who was the 44th president of the United States from 2009 to 2017. "
                        + "A member of the Democratic Party, he was the first African-American president in American history. "
                        + "Obama previously served as a U.S. senator representing Illinois from 2005 to 2008 and as an Illinois state senator from 1997 to 2004."
        );
        jCas.setDocumentLanguage("en");

        composer.run(jCas, "duui-spacy-eos:0.1.0/en");

        printResults(jCas);
    }

    @Test
    public void testNoLang() throws Exception {
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(
                "Barack Hussein Obama II (born August 4, 1961) is an American politician who was the 44th president of the United States from 2009 to 2017. "
                        + "A member of the Democratic Party, he was the first African-American president in American history. "
                        + "Obama previously served as a U.S. senator representing Illinois from 2005 to 2008 and as an Illinois state senator from 1997 to 2004."
        );

        composer.run(jCas, "duui-spacy-eos:0.1.0/no-lang");

        printResults(jCas);
    }

    private static void printResults(JCas jCas) {
        for (TOP annotation : JCasUtil.select(jCas, TOP.class)) {
            System.out.print(annotation.toString(2));
            if (annotation instanceof Sentence) {
                System.out.printf("%n  text: '%s'%n%n", ((Sentence) annotation).getCoveredText());
            } else {
                System.out.printf("%n%n");
            }
        }
    }
}