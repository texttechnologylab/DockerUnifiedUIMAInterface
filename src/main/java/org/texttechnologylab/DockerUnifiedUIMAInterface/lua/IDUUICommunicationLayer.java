package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.List;
import java.util.Map;

public interface IDUUICommunicationLayer {
    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters) throws CompressorException, IOException, SAXException;
    public void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException;
    public IDUUICommunicationLayer copy();
    public ByteArrayInputStream merge(List<ByteArrayInputStream> results);
}
