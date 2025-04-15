package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.UIMAFramework;
import org.apache.uima.util.Logger;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.bb.Rect;
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

    public Logger getLogger() {
        return UIMAFramework.getLogger(this.getClass());
    }

    /**
     * @return The bounding box {@link Rect rectangle} for this element.
     */
    public Rect getRect() {
        return new Rect(top, right, bottom, left);
    }
}
