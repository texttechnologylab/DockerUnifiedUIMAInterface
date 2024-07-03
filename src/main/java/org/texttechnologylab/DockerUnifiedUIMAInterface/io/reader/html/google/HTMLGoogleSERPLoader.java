package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.google;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.MetaDataStringField;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.type.id.URL;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

public class HTMLGoogleSERPLoader extends DefaultHandler {
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
        StringBuilder sofa = new StringBuilder();

        // Suchanfrage im Dokumenttext und als Meta hinzufügen
        String query = doc.select("textarea").text();
        int queryBegin = sofa.length();
        sofa.append(query);
        int queryEnd = sofa.length();
        MetaDataStringField queryMeta = new MetaDataStringField(jCas, queryBegin, queryEnd);
        queryMeta.setKey("google_serp_query");
        queryMeta.setValue(query);
        queryMeta.addToIndexes();
        Paragraph queryParagraph = new Paragraph(jCas, queryBegin, queryEnd);
        queryParagraph.addToIndexes();

        // Suchergebnisse extrahieren
        for (Element ser : doc.select("div.g")) {
            String title = ser.select("h3").text();
            String link = ser.select(".yuRUbf a").attr("href");
            String snippet = ser.select(".VwiC3b").text();

            if (!sofa.isEmpty()) {
                sofa.append("\n\n\n\n");
            }
            int begin = sofa.length();
            sofa.append(title).append("\n\n").append(snippet);
            int end = sofa.length();

            Paragraph paragraph = new Paragraph(jCas, begin, end);
            paragraph.addToIndexes();

		AnnotationComment originalUrl = new AnnotationComment(jCas);
		originalUrl.setKey("google_serp_link");
		originalUrl.setValue(link);
		originalUrl.setReference(paragraph);
		originalUrl.addToIndexes();

            try {
                java.net.URL url = new URI(link).toURL();
                URL urlAnno = new URL(jCas, begin, end);
                urlAnno.setScheme(url.getProtocol());
                try {
                    String[] userInfo = url.getUserInfo().split(":", 2);
                    if (userInfo.length == 2) {
                        urlAnno.setUser(userInfo[0]);
                        urlAnno.setPassword(userInfo[1]);
                    } else {
                        urlAnno.setUser(url.getUserInfo());
                    }
                } catch (Exception e) {
                    // ignored
                }
                urlAnno.setHost(url.getHost());
                if (url.getPort() != -1) {
                    urlAnno.setPort(url.getPort());
                }
                urlAnno.setPath(url.getPath());
                urlAnno.setQuery(url.getQuery());
                urlAnno.setFragment(url.getRef());
                urlAnno.addToIndexes();

            }
            catch (Exception e) {
                // ignored
            }
        }

        jCas.setDocumentText(sofa.toString());
    }
}
