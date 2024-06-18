package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class HTMLReadabilityLoader extends DefaultHandler {
    public static JCas load(Path filename, String language) throws ParserConfigurationException, SAXException, IOException, UIMAException {
        JCas jCas = JCasFactory.createJCas();

	if (language != null) {
        	jCas.setDocumentLanguage(language);
	}

        if (filename.toString().endsWith(".gz")) {
            load(new GZIPInputStream(Files.newInputStream(filename)), jCas);
        } else {
            load(Files.newInputStream(filename), jCas);
        }

        return jCas;
    }

    public static void load(InputStream stream, JCas jCas) throws ParserConfigurationException, SAXException, UIMAException, IOException {
        // NOTE jsoup adds html/head/body tags if not present
        Document doc = Jsoup.parse(stream, null, "");
        List<String> texts = new ArrayList<>();
        traverse(doc, texts);

        StringBuilder sofa = new StringBuilder();
        for (String text : texts) {
            if (sofa.length() > 0) {
                sofa.append(" ");
            }
            int begin = sofa.length();
            sofa.append(text);
            int end = sofa.length();

            Paragraph paragraph = new Paragraph(jCas, begin, end);
            paragraph.addToIndexes();
        }

        jCas.setDocumentText(sofa.toString());
    }

    private static void traverse(Element elem, List<String> texts) {
        List<Element> children = elem.children();
        if (children.isEmpty()) {
            String text = elem.text().strip();
            if (!text.isEmpty()) {
                texts.add(text);
            }
        }
        else {
            for (Element child : elem.children()) {
                // NOTE that getElementsByTag returns "own" tag thus using child p, li, ... as paragraphs
                if (!child.getElementsByTag("div").isEmpty()) {
                    traverse(child, texts);
                } else {
                    String text = child.text().strip();
                    if (!text.isEmpty()) {
                        texts.add(text);
                    }
                }
            }
        }
    }
}
