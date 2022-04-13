package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DUUIFallbackCommunicationLayer implements IDUUICommunicationLayer {
    public void serialize(JCas jc, ByteArrayOutputStream out) throws CompressorException, IOException, SAXException {
        JSONObject obj = new JSONObject();
        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(), null, arr);

        StringWriter writer = new StringWriter();
        TypeSystemUtil.typeSystem2TypeSystemDescription(jc.getTypeSystem()).toXML(writer);
        String typesystem = writer.getBuffer().toString();

        String cas = arr.toString();
        obj.put("cas", cas);
        obj.put("typesystem", typesystem);
        obj.put("typesystem_hash", typesystem.hashCode());
        obj.put("cas_hash", cas.hashCode());
        obj.put("compression","none");
        out.write(obj.toString().getBytes(StandardCharsets.UTF_8));
    }

    public void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException {
        String body = new String(input.readAllBytes(), Charset.defaultCharset());
        JSONObject response = new JSONObject(body);
        if (response.has("cas") || response.has("error")) {
            jc.reset();
            String deserialized = response.getString("cas");
            XmiCasDeserializer.deserialize(new ByteArrayInputStream(deserialized.getBytes(StandardCharsets.UTF_8)), jc.getCas(), true);
        } else {
            throw new InvalidObjectException("Response is not in the right format!");
        }
    }

    public IDUUICommunicationLayer copy() {
        return new DUUIFallbackCommunicationLayer();
    }
}
