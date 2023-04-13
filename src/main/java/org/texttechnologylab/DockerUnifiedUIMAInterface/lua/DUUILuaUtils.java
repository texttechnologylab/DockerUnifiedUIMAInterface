package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.uima.jcas.JCas;

public class DUUILuaUtils {
    // TODO Different from Luas "string.len"?
    static public int getDocumentTextLength(JCas jCas) {
        return jCas.getDocumentText().length();
    }
}
