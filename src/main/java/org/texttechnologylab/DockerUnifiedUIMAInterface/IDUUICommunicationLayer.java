package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.jcas.JCas;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IDUUICommunicationLayer {
    public void serialize(JCas jc, OutputStream out) throws CompressorException, IOException, SAXException;
    public void deserialize(JCas jc, InputStream input);
}
