package org.texttechnologylab.DockerUnifiedUIMAInterface.lua_process;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.resource.ResourceInitializationException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.docker.DockerTestContainerManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSpacyEos {
    final DockerTestContainerManager container = new DockerTestContainerManager(
            "docker.texttechnologylab.org/duui-spacy-eos:latest",
            6000
    );

    DUUIComposer composer;

    @BeforeAll
    public void setUp() throws Exception {
        composer = new DUUIComposer()
                .withLuaContext(
                        new DUUILuaContext()
                                .withJsonLibrary())
                .withSkipVerification(true);
        composer.addDriver(new DUUIRemoteDriver(10000));
        composer.add(new DUUIRemoteDriver.Component("http://localhost:%d".formatted(container.getPort())));
    }

    @AfterAll
    public void shutdown() throws Exception {
        if (composer != null)
            composer.shutdown();

        container.close();
    }

    @Test
    public void testEnglish() throws Exception {
        JCas jCas = getJCas();
        jCas.setDocumentLanguage("en");

        composer.run(jCas, "duui-spacy-eos:0.1.0/en");

        printResults(jCas);
    }

    @NotNull
    private static JCas getJCas() throws ResourceInitializationException, CASException {
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(
                "Barack Hussein Obama II (born August 4, 1961) is an American politician who was the 44th president of the United States from 2009 to 2017. "
                        + "A member of the Democratic Party, he was the first African-American president in American history. "
                        + "Obama previously served as a U.S. senator representing Illinois from 2005 to 2008 and as an Illinois state senator from 1997 to 2004."
        );
        return jCas;
    }

    @Test
    public void testNoLang() throws Exception {
        JCas jCas = getJCas();

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