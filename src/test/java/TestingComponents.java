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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.annotation.type.Taxon;
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
public class TestingComponents {

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

    public static JCas getCasDE() throws ResourceInitializationException, CASException {
        // init a CAS with a static text.

        Set<String> set = new HashSet<>();
        set.add("Die Tannen (Abies) bilden eine Pflanzengattung in der Familie der Kieferngewächse (Pinaceae). Je nach taxonomischer Auffassung gibt es entweder 40 Arten oder 47 Arten sowie etliche Gruppen hybriden Ursprungs oder Varietäten, die alle in gemäßigten Gebieten der Nordhalbkugel und zumeist in Gebirgsregionen vorkommen.");


        set.add("Alle Tannen-Arten sind immergrüne tiefwurzelnde Bäume mit einem geraden, säulenförmigen Stamm. Die konische Krone wird aus regelmäßigen Etagen von relativ kurzen, horizontalen Ästen gebildet. Wenige Arten bleiben mit einer Wuchshöhe von 20 Metern relativ klein (Abies koreana), die meisten Arten sind aber in ihren Lebensraum dominante Waldbäume und gehören dann oft zu den größten Waldbaumarten (die Europäische Weiß-Tanne gilt als größter Baum des Kontinents). So erreichen die meisten Tannen Wuchshöhen zwischen 40 und 70 Metern, für manche amerikanische Arten sind aber auch Wuchshöhen von 90 Metern verbürgt (Abies grandis, Abies procera). Der Stamm der monopodial wachsenden Tannen wird in der Regel 1 bis 2 Meter dick (bis zu 3 Meter bei Abies procera und Abies spectabilis). Sämlinge besitzen vier bis zehn Keimblätter (Kotyledonen).");

        set.add("Mit wenigen Ausnahmen haben Tannen ein pfahlförmiges Wurzelsystem. Dieses wird unabhängig vom Bodentyp entwickelt. Für die Purpur-Tanne (Abies amabilis) ist dagegen ein flaches Wurzelsystem kennzeichnend. Sie ist daher wenig sturmresistent. Von allen Nadelbaumarten leisten die Tannen-Arten auf den waldbaulich schwierigen, sauerstoffarmen Böden (Staunässe, Pseudogley, Ton) den weitaus besten vertikalen Aufschluss.");

        set.add("Die Borke ist in der Jugend zumeist glatt, oft grau, nur bei Abies squamata auch schon in der Jugend schuppig, zerfällt aber im Alter zumeist in kleine Platten (Abies alba, Abies nordmanniana, Abies procera).");

        set.add("Die nadelförmigen Blätter sind flach und leicht biegsam und tragen auf der Unterseite oft zwei helle Stoma-Bänder. Die Nadeln werden acht bis elf Jahre alt. Mit ihrem verbreiterten Fuß sitzen sie unmittelbar auf den Ästen (siehe Bild). Sie unterscheiden sich dadurch z. B. von Fichten (Picea).");

        set.add("Tannen-Arten sind einhäusig getrenntgeschlechtig (monözisch), es gibt weibliche und männliche Zapfen an einem Pflanzenexemplar. Die Zapfen finden sich nur in den obersten Zweigen am Wipfel und stehen immer aufrecht am Zweig (im Gegensatz zu den hängenden und als Ganzes herabfallenden Fichtenzapfen). Die Achse (Spindel) des Zapfens verbleibt am Baum, während die Schuppen einzeln abfallen. Folglich können auch keine herabgefallenen Tannenzapfen gesammelt werden. Die geflügelten Samen reifen im Zapfen. Die Stellung und Entwicklung der Zapfen ist nicht nur für die Gattungsabgrenzung essentiell, ihre Form ist auch eines der wichtigsten morphologischen Unterscheidungsmerkmale zwischen den einzelnen Arten.");

        set.add("Tannenholz ist weich, in der Regel geruchslos, cremeweiß bis lohfarben. Kern- und Splintholz sind praktisch nicht unterscheidbar. Die Holzfasern sind gerade, mit einem graduellen Übergang zwischen Früh- und Spätholz. Harzkanäle werden in der Regel nicht gebildet. ");

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

        pCas.setDocumentLanguage("de");
        pCas.setDocumentText(sb.toString());


        // Define some metadata to serialize the CAS with the xmi writer
        DocumentMetaData dmd = new DocumentMetaData(pCas);
        dmd.setDocumentId("Tanne");
        dmd.setDocumentTitle("Tanne");
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
    @DisplayName("taxoNerd")
    public void taxoNerd() throws Exception {
// reset existing pipeline-components

        pComposer.resetPipeline();

        JCas tCas = getCasDE();

        pComposer.add(new DUUIRemoteDriver.Component("http://taxonerd.service.component.duui.texttechnologylab.org")
                .withScale(iWorkers)
                .withParameter("linker", "ncbi_taxonomy")
//                .withParameter("linker", "taxref")
                .withParameter("threshold", "0.5")
                .build().withTimeout(100000));
//        pComposer.add(new DUUIDockerDriver.Component("duui-taxonerd:1.5")
//                .withImageFetching()
//                .withScale(iWorkers)
//                .build());

        pComposer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());


        pComposer.run(tCas);
        pComposer.shutdown();

        JCasUtil.select(tCas, Taxon.class).stream().forEach(System.out::println);


    }

}
