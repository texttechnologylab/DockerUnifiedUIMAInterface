package slurmDriver;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ParlbertTopicGermanTest {

    public static final String dijkstraExampleText = "Der Algorithmus von Dijkstra (nach seinem Erfinder Edsger W. Dijkstra) ist ein Algorithmus aus der Klasse der Greedy-Algorithmen[1] und löst das Problem der kürzesten Pfade für einen gegebenen Startknoten. " +
            "Er berechnet somit einen kürzesten Pfad zwischen dem gegebenen Startknoten und einem der (oder allen) übrigen Knoten in einem kantengewichteten Graphen (sofern dieser keine Negativkanten enthält)." +
            "Für unzusammenhängende ungerichtete Graphen ist der Abstand zu denjenigen Knoten unendlich, zu denen kein Pfad vom Startknoten aus existiert. Dasselbe gilt auch für gerichtete nicht stark zusammenhängende Graphen. Dabei wird der Abstand synonym auch als Entfernung, Kosten oder Gewicht bezeichnet.";

    @Test
    public void test() throws Exception {

        int iWorkers = 1;
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.


        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver(1000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(false);
        composer.addDriver(docker_driver, remoteDriver,uima_driver);

        boolean useDockerImage = false;
        if (useDockerImage){
             composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/parlbert-topic-german:latest")
                    .withScale(iWorkers)
                    .build());
        }else{
            composer.add(new DUUIRemoteDriver.Component("http://localhost:20000")
                .withScale(iWorkers)
                .build());
            composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                    XmiWriter.PARAM_TARGET_LOCATION, "/tmp/slurm",
                    XmiWriter.PARAM_PRETTY_PRINT, true,
                    XmiWriter.PARAM_OVERWRITE, true,
                    XmiWriter.PARAM_VERSION, "1.1"
            )).build());
        }

        // Create basic test jCas.
        JCas jCas = JCasFactory.createText(dijkstraExampleText, "de");

        new Sentence(jCas, 0, 206).addToIndexes();
        new Sentence(jCas, 206, 402).addToIndexes();
        new Sentence(jCas, 402, 544).addToIndexes();
        new Sentence(jCas, 544, 616).addToIndexes();
        new Sentence(jCas, 616, 699).addToIndexes();

        composer.run(jCas, "test");

        // Print Result
        Collection<CategoryCoveredTagged> categoryCoveredTaggeds = JCasUtil.select(jCas, CategoryCoveredTagged.class).stream().sorted((c1, c2) -> c1.getBegin()-c2.getBegin()).collect(Collectors.toList());
        for(CategoryCoveredTagged categoryCoveredTagged: categoryCoveredTaggeds){
            System.out.println(categoryCoveredTagged.getBegin() + " - " + categoryCoveredTagged.getEnd() + " " + categoryCoveredTagged.getValue() + ": " + categoryCoveredTagged.getScore());
        }



    }

    @Test
    public void singleTokenizedTestDe() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(
                        new DUUILuaContext()
                                .withJsonLibrary()
                )
                .withSkipVerification(true);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        composer.addDriver(remote_driver,uima_driver);

        composer.add(
                new DUUIRemoteDriver.Component("http://127.0.0.1:20000")
                        .withParameter("use_existing_tokens", String.valueOf(true))
        );
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/slurm",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1"
        )).build());

        String language = "de";

        JCas cas = JCasFactory.createJCas();
        cas.setDocumentText("Das ist ein IPhone von Apple. Und das ist ein iMac.");
        cas.setDocumentLanguage(language);

        int[][] tokens1 = new int[][]{
                new int[]{0, 3}, //Das
                new int[]{4, 7}, //ist
                new int[]{8, 11}, //ein
                new int[]{12, 18}, //IPhone
                new int[]{19, 22}, //von
                new int[]{23, 28}, //Apple
                new int[]{28, 29}, //.
                new int[]{30, 33}, //Und
                new int[]{34, 37}, //das
                new int[]{38, 41}, //ist
                new int[]{42, 45}, //ein
                new int[]{46, 50}, //iMac
                new int[]{50, 51} //.
        };

        for (int[] tokenPos : tokens1) {
            Token token = new Token(cas, tokenPos[0], tokenPos[1]);
            token.addToIndexes();
        }

        composer.run(cas);

    }
}
