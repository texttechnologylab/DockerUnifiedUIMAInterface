package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.IOException;

public class DUUIIdentityMapper implements IDUUIMapper {
    public JSONObject map(JSONObject previous, DUUIEither input) throws CompressorException, IOException, SAXException {
        return new JSONObject();
    }

    public void reduce(String input, DUUIEither output) {
        output.updateStringBuffer(input);
    }
 }
