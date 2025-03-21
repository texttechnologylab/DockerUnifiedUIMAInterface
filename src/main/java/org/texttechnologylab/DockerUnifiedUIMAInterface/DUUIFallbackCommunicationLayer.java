package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class DUUIFallbackCommunicationLayer implements IDUUICommunicationLayer {
    public SerializeOutput serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters, String sourceView) throws CompressorException, IOException, SAXException, CASException {
        JSONObject obj = new JSONObject();
        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getView(sourceView).getCas(), null, arr);

        StringWriter writer = new StringWriter();
        TypeSystemUtil.typeSystem2TypeSystemDescription(jc.getTypeSystem()).toXML(writer);
        String typesystem = writer.getBuffer().toString();

        String cas = arr.toString();
        obj.put("cas", cas);
        obj.put("typesystem", typesystem);
        obj.put("typesystem_hash", typesystem.hashCode());
        obj.put("cas_hash", cas.hashCode());
        obj.put("compression","none");
        obj.put("params",parameters);
        out.write(obj.toString().getBytes(StandardCharsets.UTF_8));

        return null;
    }

    public void deserialize(JCas jc, ByteArrayInputStream input, String targetView) throws IOException, SAXException {
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

    @Override
    public SerializeOutput serialize(JCas jc, ByteArrayOutputStream out, Map<String, String> parameters) throws CompressorException, IOException, SAXException, CASException {
        return serialize(jc, out, parameters, "_InitialView");
    }

    @Override
    public void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException, CASException {
        deserialize(jc, input, "_InitialView");
    }

    public IDUUICommunicationLayer copy() {
        return new DUUIFallbackCommunicationLayer();
    }

    @Override
    public ByteArrayInputStream merge(List<ByteArrayInputStream> results) {
        return null;
    }

    @Override
    public String myLuaTestMerging() {
        // das ist eine Test-Funktion
        return "Hallo ich bin in DUUIFallbackCommunicationLayer";
    }
}
