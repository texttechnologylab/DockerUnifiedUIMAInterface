package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.xml.sax.Attributes;

abstract public class AbstractStructuralElement {
    final int top;
    final int bottom;
    final int left;
    final int right;

    public AbstractStructuralElement(Attributes attributes) {
        this.top = Utils.parseInt(attributes.getValue("t"));
        this.bottom = Utils.parseInt(attributes.getValue("b"));
        this.left = Utils.parseInt(attributes.getValue("l"));
        this.right = Utils.parseInt(attributes.getValue("r"));
    }
}
