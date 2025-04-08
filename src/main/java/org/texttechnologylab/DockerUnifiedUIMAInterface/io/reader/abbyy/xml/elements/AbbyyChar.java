package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.xml.sax.Attributes;

public class AbbyyChar extends AbstractStructuralElement {

    private final boolean isWordFromDictionary;
    private final boolean isWordNormal;
    private final boolean isWordNumeric;
    private final int suspicious;

    public String value = "";

    public AbbyyChar(Attributes attributes) {
        super(attributes);
        isWordFromDictionary = Utils.parseBoolean(attributes.getValue("wordFromDictionary"));
        isWordNormal = Utils.parseBoolean(attributes.getValue("wordNormal"));
        isWordNumeric = Utils.parseBoolean(attributes.getValue("wordNumeric"));
        suspicious = Utils.parseBoolean(attributes.getValue("suspicious")) ? 1 : 0;
    }

    public boolean isWordFromDictionary() {
        return isWordFromDictionary;
    }

    public boolean isWordNormal() {
        return isWordNormal;
    }

    public boolean isWordNumeric() {
        return isWordNumeric;
    }

    public int getSuspicious() {
        return suspicious;
    }
}
