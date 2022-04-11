package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.jcas.JCas;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Alexander Leonhardt
 */
public interface IDUUICommunicationLayer {
    /**
     *
     * @param jc
     * @param out
     * @throws CompressorException
     * @throws IOException
     * @throws SAXException
     */
    public void serialize(JCas jc, OutputStream out) throws CompressorException, IOException, SAXException;

    /**
     *
     * @param jc
     * @param input
     * @throws IOException
     * @throws SAXException
     */
    public void deserialize(JCas jc, InputStream input) throws IOException, SAXException;

    /**
     *
     * @return
     */
    public IDUUICommunicationLayer copy();
}
