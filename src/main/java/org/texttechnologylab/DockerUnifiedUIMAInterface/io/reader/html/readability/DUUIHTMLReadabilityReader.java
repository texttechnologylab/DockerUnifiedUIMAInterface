package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.html.readability;

import org.apache.uima.collection.CollectionException;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.io.JCasResourceCollectionReader_ImplBase;
import org.dkpro.core.api.resources.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;

public class DUUIHTMLReadabilityReader extends JCasResourceCollectionReader_ImplBase {
    @Override
    public void getNext(JCas jCas) throws IOException, CollectionException {
        Resource res = nextFile();
        initCas(jCas, res);
        try (InputStream is = CompressionUtils.getInputStream(res.getLocation(), res.getInputStream())) {
            HTMLReadabilityLoader.load(is, jCas);
        } catch (Exception e) {
            //throw new CollectionException(e);
            System.err.println("Fehler bei: " + res.getLocation());
            jCas.setDocumentText("");
        }
    }
}
