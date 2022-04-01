package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.IOException;

public interface IDUUIMapper {
    public JSONObject map(JSONObject previous, DUUIEither input) throws CompressorException, IOException, SAXException;
    public void reduce(String result, DUUIEither output);
}
