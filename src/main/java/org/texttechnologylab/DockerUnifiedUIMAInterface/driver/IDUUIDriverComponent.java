package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.io.IOException;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature.DependencyType;
import org.xml.sax.SAXException;

public interface IDUUIDriverComponent<Component extends IDUUIDriverComponent<Component>> {

    public Component getComponent(); 

    public DUUIPipelineComponent getPipelineComponent();

    public default Component withParameter(String key, String value) {
        getPipelineComponent().withParameter(key,value);
        return getComponent();
    }
    
    public default Component withDescription(String description) {
        getPipelineComponent().withDescription(description);
        return getComponent();
    }

    public default Component withScale(int scale) {
        getPipelineComponent().withScale(scale);
        return getComponent();
    }

    public default Component withSignature(DependencyType firstOrLast) {
        getPipelineComponent().withSignature(firstOrLast);
        return getComponent();
    }

    public  DUUIPipelineComponent build()  throws IOException, SAXException; 
}
