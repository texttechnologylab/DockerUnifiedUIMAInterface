package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.jcas.JCas;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;

public interface IDUUICommunicationLayer {
    public void serialize(JCas jc, ByteArrayOutputStream out) throws CompressorException, IOException, SAXException;
    public void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException;
    public IDUUICommunicationLayer copy();
}
