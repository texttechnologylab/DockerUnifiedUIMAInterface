package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.bytes;

import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.ByteArray;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.api.io.JCasResourceCollectionReader_ImplBase;
import org.dkpro.core.api.resources.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class DUUIBytesReader extends JCasResourceCollectionReader_ImplBase {
    public static final String PARAM_MIME_TYPE = "mimeType";
    @ConfigurationParameter(name = PARAM_MIME_TYPE, mandatory = false)
    protected String mimeType;

    @Override
    public void getNext(JCas jCas) throws CollectionException {
        Resource res = nextFile();
        initCas(jCas, res);
        try (InputStream is = CompressionUtils.getInputStream(res.getLocation(), res.getInputStream())) {
            byte[] content = is.readAllBytes();
            ByteArray data = new ByteArray(jCas, content.length);
            data.copyFromArray(content, 0, 0, content.length);
            jCas.setSofaDataArray(data, mimeType);
        } catch (Exception e) {
            throw new CollectionException(e);
        }
    }

    public static void load(JCas jCas, Path path, String mimeType) throws IOException, ResourceInitializationException, CASException {
        try (InputStream io = Files.newInputStream(path)) {
            try (InputStream is = CompressionUtils.getInputStream(path.toString(), io)) {
                byte[] content = is.readAllBytes();
                ByteArray data = new ByteArray(jCas, content.length);
                data.copyFromArray(content, 0, 0, content.length);
                jCas.setSofaDataArray(data, mimeType);
            }
        }
    }

    public static JCas load(Path path, String mimeType) throws IOException, ResourceInitializationException, CASException {
        JCas jCas = JCasFactory.createJCas();
        load(jCas, path, mimeType);
        return jCas;
    }
}
