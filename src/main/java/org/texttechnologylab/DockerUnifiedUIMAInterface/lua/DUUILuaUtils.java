package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import org.apache.uima.jcas.JCas;

/**
 * Auxiliary class for the UTF16 problem
 *
 * @author Daniel Baumartz
 */
public class DUUILuaUtils {
    // TODO Different from Luas "string.len"?
    static public int getDocumentTextLength(JCas jCas) {
        return jCas.getDocumentText().length();
    }
}
