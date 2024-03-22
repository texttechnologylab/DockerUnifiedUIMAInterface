package org.texttechnologylab.DockerUnifiedUIMAInterface.io.format;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

import java.io.IOException;
import java.io.InputStream;

public interface IDUUIFormat {
    void load(InputStream stream, JCas jCas) throws UIMAException, IOException;
}
