package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.luaj.vm2.LuaValue;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIHttpRequestHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class DUUIFallbackCommunicationLayer implements IDUUICommunicationLayer {
    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters, String sourceView) throws CommunicationLayerException, CASException {
        try {
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
            obj.put("compression", "none");
            obj.put("params", parameters);
            out.write(obj.toString().getBytes(StandardCharsets.UTF_8));
        } catch (SAXException e) {
            throw new CommunicationLayerException("Failed to serialize CAS to XMI!", e);
        } catch (IOException e) {
            throw new CommunicationLayerException("Failed to write serialized CAS to output stream!", e);
        }
    }

    public void deserialize(JCas jc, ByteArrayInputStream input, String targetView) throws CommunicationLayerException, CASException {
        try {
            String body = new String(input.readAllBytes(), Charset.defaultCharset());
            JSONObject response = new JSONObject(body);
            if (response.has("cas") || response.has("error")) {
                jc.reset();
                String deserialized = response.getString("cas");
                XmiCasDeserializer.deserialize(new ByteArrayInputStream(deserialized.getBytes(StandardCharsets.UTF_8)), jc.getCas(), true);
            } else {
                throw new InvalidObjectException("Response is not in the right format!");
            }
        } catch (SAXException | IOException e) {
            throw new CommunicationLayerException("Failed to deserialize CAS from XMI!", e);
        }
    }

    @Override
    public void serialize(JCas jc, ByteArrayOutputStream out, Map<String, String> parameters) throws CommunicationLayerException, CASException {
        serialize(jc, out, parameters, "_InitialView");
    }

    @Override
    public void deserialize(JCas jc, ByteArrayInputStream input) throws CommunicationLayerException, CASException {
        deserialize(jc, input, "_InitialView");
    }

    @Override
    public void process(JCas jCas, DUUIHttpRequestHandler handler, Map<String, String> parameters, JCas targetCas) throws CommunicationLayerException, CASException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsProcess() {
        return false;
    }

    public boolean supportsSerialize() {
        return true;
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
