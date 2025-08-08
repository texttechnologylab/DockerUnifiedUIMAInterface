package org.texttechnologylab.DockerUnifiedUIMAInterface.lua_process;

import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.docker.DockerTestContainerManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.annotation.biofid.gnfinder.MetaData;
import org.texttechnologylab.annotation.biofid.gnfinder.Taxon;

import java.io.IOException;
import java.net.URISyntaxException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGNFinderLite {
    final DockerTestContainerManager container = new DockerTestContainerManager(
            "docker.texttechnologylab.org/duui-lite-gnfinder:latest"
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
    }

    @AfterAll
    public void shutdown() throws Exception {
        if (composer != null)
            composer.shutdown();

        container.close();
    }

    private DUUIRemoteDriver.Component getComponent() throws URISyntaxException, IOException {
        return new DUUIRemoteDriver.Component("http://localhost:%d".formatted(container.getPort()));
    }

    private static JCas getJCas() throws ResourceInitializationException, CASException {
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(
                String.join("\n", "Nach Schluß des Congresses ist eine längere Excursion vorgesehen, auf welcher die Inseln an der Küste von Pembrokshire besucht werden.", "Dieser Ausflug dürfte besonders interessant werden, weil sich hier große Brutkolonien von Puffinus puffinus und verschiedener Alcidae befinden.", "Auch Thalassidroma pelagica dürfte hier angetroffen werden.", "Bei günstigem Wetter ist ferner der Besuch einer Brutkolonie von Sula bassana vorgesehen.", "Homo sapiens sapiens."));
        jCas.setDocumentLanguage("de");
        return jCas;
    }

    @Test
    public void test_default() throws Exception {
        composer.resetPipeline();
        composer.add(
                getComponent()
                        .build()
        );

        JCas jCas = getJCas();
        composer.run(jCas);

        printResults(jCas);
    }

    @Test
    public void test_with_noBayes() throws Exception {
        composer.resetPipeline();
        composer.add(
                getComponent()
                        .withParameter("noBayes", "true")
                        .build()
        );

        JCas jCas = getJCas();
        composer.run(jCas);

        printResults(jCas);
    }

    @Test
    public void test_with_allMatches() throws Exception {
        composer.resetPipeline();
        composer.add(
                getComponent()
                        // Catalogue of Life and GBIF
                        .withParameter("sources", "[1, 11]")
                        .withParameter("allMatches", "true")
                        .build()
        );

        JCas jCas = getJCas();
        composer.run(jCas);

        printResults(jCas);
    }

    @Test
    public void test_with_oddsDetails() throws Exception {
        composer.resetPipeline();
        composer.add(
                getComponent()
                        .withParameter("oddsDetails", "true")
                        .build()
        );

        JCas jCas = getJCas();
        composer.run(jCas);

        printResults(jCas);
    }

    private static void printResults(JCas jCas) {
        System.out.println(jCas.select(MetaData.class).findFirst().get().toString(2));
        System.out.println();

        for (Taxon tx : jCas.select(Taxon.class)) {
            System.out.print(tx.toString(2));
            System.out.println("\n  > coveredText: \"" + tx.getCoveredText() + "\"\n");
            System.out.println();
        }
    }
}
