import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;

@Nested
public class CorporaTests {

    static DUUIComposer composer = null;

    static JCas pCas = null;

    @BeforeAll
    public static void init() throws IOException, URISyntaxException, UIMAException, SAXException {


        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(1);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(10000)
                .withOverlap(500);


        DUUIKubernetesDriver kubernetes_driver = new DUUIKubernetesDriver();
        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();

        composer.addDriver(kubernetes_driver, docker_driver, uima_driver, remote_driver);

    }

    @BeforeEach
    public void prepareJCas() throws ResourceInitializationException, CASException {
        pCas = JCasFactory.createText("World War II or the Second World War was a global conflict that lasted from 1939 to 1945. The vast majority of the world's countries, including all the great powers, fought as part of two opposing military alliances: the Allies and the Axis. Many participating countries invested all available economic, industrial, and scientific capabilities into this total war, blurring the distinction between civilian and military resources. Aircraft played a major role, enabling the strategic bombing of population centres and delivery of the only two nuclear weapons ever used in war. It was by far the deadliest conflict in history, resulting in 70–85 million fatalities. Millions died due to genocides, including the Holocaust, as well as starvation, massacres, and disease. In the wake of Axis defeat, Germany, Austria, and Japan were occupied, and war crime tribunals were conducted against German and Japanese leaders.\n" +
                "\n" +
                "The causes of the war are debated; contributing factors included the rise of fascism in Europe, the Spanish Civil War, the Second Sino-Japanese War, Soviet–Japanese border conflicts, and tensions in the aftermath of World War I. World War II is generally considered to have begun on 1 September 1939, when Nazi Germany, under Adolf Hitler, invaded Poland. The United Kingdom and France declared war on Germany on 3 September. Under the Molotov–Ribbentrop Pact of August 1939, Germany and the Soviet Union had partitioned Poland and marked out their \"spheres of influence\" across Finland, Estonia, Latvia, Lithuania, and Romania. From late 1939 to early 1941, in a series of campaigns and treaties, Germany conquered or controlled much of continental Europe in a military alliance called the Axis with Italy, Japan, and other countries. Following the onset of campaigns in North and East Africa, and the fall of France in mid-1940, the war continued primarily between the European Axis powers and the British Empire, with the war in the Balkans, the aerial Battle of Britain, the Blitz of the UK, and the Battle of the Atlantic. In June 1941, Germany led the European Axis powers in an invasion of the Soviet Union, opening the Eastern Front, the largest land theatre of war in history.");
    }

    @ParameterizedTest
    @DisplayName("Toxic")
    @ValueSource(strings = {"cardiffnlp/tweet-topic-latest-multi", "classla/xlm-roberta-base-multilingual-text-genre-classifier", "chkla/parlbert-topic-german", "ssharoff/genres", "manifesto-project/manifestoberta-xlm-roberta-56policy-topics-context-2023-1-1"})
    public void testToxic(String sModel) throws Exception {

        composer.resetPipeline();

        try {
            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                            .withImageFetching()
                            .withScale(1)
                            .build()
            );

            composer.add(
                    new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-topic:0.1.3")
                            .withImageFetching()
                            .withScale(1)
                            .withParameter("model_name", sModel)
//                            .withParameter("selection", "text")
                            .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
                            .build()
            );

            composer.run(pCas);

            assert JCasUtil.select(pCas, CategoryCoveredTagged.class).stream().count() > 0;

//            JCasUtil.select(pCas, CategoryCoveredTagged.class).forEach(a -> {
//                System.out.println(a);
//            });
        } finally {
            composer.shutdown();
        }

    }

}
