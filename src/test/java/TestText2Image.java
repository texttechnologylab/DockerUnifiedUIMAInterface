import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.annotation.type.Image;
import org.xml.sax.SAXException;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

@Nested
public class TestText2Image {

    // common class-attributes
    private static DUUIComposer pComposer = null;
    private static int iWorkers = 1;

    /**
     * Initialization of DUUI for each test, saves lines of code.
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws UIMAException
     * @throws SAXException
     */
    @BeforeAll
    public static void init() throws IOException, URISyntaxException, UIMAException, SAXException {

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        pComposer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIPodmanDriver podmanDriver = new DUUIPodmanDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        pComposer.addDriver(uima_driver, remoteDriver, dockerDriver, podmanDriver);

    }

    /**
     * Initialization of a sample CAS document
     *
     * @return
     * @throws ResourceInitializationException
     * @throws CASException
     */
    public static JCas getCas() throws ResourceInitializationException, CASException {
        // init a CAS with a static text.

        Set<String> set = new HashSet<>();
        set.add("The sky, once a vast canvas of twilight blues, now churns with thick, ink-black clouds. The wind howls like a pack of unseen wolves, tearing through the ship’s sails with ruthless fingers. Waves, tall as mountains, crash against the wooden hull, sending salty spray into the air like shattered glass. Thunder growls in the distance, a monstrous drumroll announcing the sky’s electric rage. The ship groans, tilting dangerously, as the sea grips it in a cold, merciless embrace.");
        set.add("Golden sunlight spills over the rolling meadow, painting the tall grass in strokes of amber and jade. Wildflowers sway lazily, their petals catching the light like tiny stained-glass windows. A soft breeze whispers through the leaves of an old oak, its gnarled branches stretching skyward like the arms of a storyteller. Bees hum drowsily among the blossoms, their tiny wings shimmering. Somewhere in the distance, a brook gurgles over smooth stones, its song a lullaby to the earth awakening from winter’s slumber.");
        set.add("Neon signs flicker like fireflies caught in a concrete jungle, their colors bleeding onto wet sidewalks. The air is thick with the scent of rain, street food, and exhaust, an intoxicating blend of life in motion. Taxis streak past like shooting stars, their headlights carving golden rivers into the asphalt. Music drifts from an open bar, mingling with the rhythmic chatter of a thousand footsteps. In the distance, skyscrapers loom, their glass facades reflecting the city’s heartbeat—a pulsing, vibrant symphony of lights and movement.");
        set.add("The house stands like a forgotten ghost, its wooden bones groaning under the weight of time. Ivy coils around its crumbling walls, nature’s fingers reclaiming what was once man’s. The windows, dark and hollow, stare like empty eyes into the surrounding wilderness. Inside, dust dances in the weak shafts of light that filter through the broken roof. The air is thick with the scent of damp wood and memories left behind. A single rocking chair sways ever so slightly, though no breeze stirs the stagnant air.");

        JCas pCas = JCasFactory.createJCas();

        StringBuilder sb = new StringBuilder();
        for (String s : set) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(s);
            Paragraph paragraph = new Paragraph(pCas);
            paragraph.setBegin(sb.toString().indexOf(s));
            paragraph.setEnd(sb.length());
            paragraph.addToIndexes();
        }

        pCas.setDocumentLanguage("en");
        pCas.setDocumentText(sb.toString());


        // Define some metadata to serialize the CAS with the xmi writer
        DocumentMetaData dmd = new DocumentMetaData(pCas);
        dmd.setDocumentId("Novels");
        dmd.setDocumentTitle("Novels");
        dmd.addToIndexes();


        return pCas;
    }

    private static void saveBase64ToImage(String base64String, String outputPath) {
        try {
            // Decode the Base64 string into a byte array
            byte[] decodedBytes = Base64.getDecoder().decode(base64String);

            // Create an image from the byte array
            ByteArrayInputStream inputStream = new ByteArrayInputStream(decodedBytes);
            BufferedImage image = ImageIO.read(inputStream);

            // Save the image to the specified output file
            File outputFile = new File(outputPath);
            ImageIO.write(image, "png", outputFile);

            System.out.println("Image saved as: " + outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void general() throws Exception {
// reset existing pipeline-components

        pComposer.resetPipeline();

        JCas tCas = getCas();
        // Please note that the Docker images are first downloaded when they are called up for the first time.
//        pComposer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-text-to-image:0.2.0")
////                .withGPU(true)
////                .withImageFetching()
//                .withParameter("model_name", "OFA-Sys/small-stable-diffusion-v0")
//                .withParameter("selection", Paragraph.class.getName())
//                .withScale(iWorkers)
//                .build().withTimeout(100000));
//
        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/duui-spacy-en_core_web_sm:latest")
                .withImageFetching()
                .withScale(iWorkers)
                .build());
//
        pComposer.add(new DUUIRemoteDriver.Component("http://geltlin.hucompute.org:9050")
                .withParameter("model_name", "OFA-Sys/small-stable-diffusion-v0")
                .withParameter("selection", Paragraph.class.getName())
                .withScale(iWorkers)
                .build().withTimeout(100000));

//        pComposer.add(new DUUIRemoteDriver.Component("http://geltlin.hucompute.org:9050")
//                .withParameter("model_name", "OFA-Sys/small-stable-diffusion-v0")
//                .withParameter("selection", Sentence.class.getName())
//                .withParameter("number_of_images", "1")
//                .withScale(iWorkers)
//                .build().withTimeout(100000));

//        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/duui-text-to-image:0.2.0")
//                .withGPU(true)
//                .withImageFetching()
//                .withParameter("model_name", "OFA-Sys/small-stable-diffusion-v0")
//                .withParameter("selection", Paragraph.class.getName())
//                .withScale(iWorkers)
//                .build().withTimeout(100000));

        pComposer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

//        // Please note that the Docker images are first downloaded when they are called up for the first time.
//        pComposer.add(new DUUIPodmanDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
//                .withImageFetching()
//                .withScale(iWorkers)
//                .build());


        pComposer.run(tCas);
        pComposer.shutdown();

        for (Image image : JCasUtil.select(tCas, Image.class)) {
            saveBase64ToImage(image.getSrc(), "/tmp/output/" + image.getBegin() + "_" + image.getEnd() + "_" + image._id());
        }

        JCasUtil.select(tCas, Image.class).stream().forEach(System.out::println);


    }

}
