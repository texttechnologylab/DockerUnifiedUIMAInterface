package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.List;
import java.util.Map;

public interface IDUUICommunicationLayer {
    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters, String sourceView) throws CompressorException, IOException, SAXException, CASException;
    public void deserialize(JCas jc, ByteArrayInputStream input, String targetView) throws IOException, SAXException, CASException;
    public IDUUICommunicationLayer copy();
    public ByteArrayInputStream merge(List<ByteArrayInputStream> results);

    String myLuaTestMerging();
}
