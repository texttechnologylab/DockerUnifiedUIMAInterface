import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

public class DUUIComposer {
    private Map<String,IDUUIDriverInterface> _drivers;
    private Vector<IDUUIPipelineComponent> _pipeline;

    private static final String DRIVER_OPTION_NAME = "duuid.composer.driver";

    public DUUIComposer() {
        _drivers = new HashMap<String,IDUUIDriverInterface>();
        _pipeline = new Vector<IDUUIPipelineComponent>();
    }

    public DUUIComposer addDriver(IDUUIDriverInterface driver) {
        _drivers.put(driver.getClass().getCanonicalName().toString(),driver);
        System.out.println(driver.getClass().toString());
        return this;
    }

    public <Y> DUUIComposer add(IDUUIPipelineComponent object, Class<Y> t) {
        object.setOption(DRIVER_OPTION_NAME,t.getCanonicalName().toString());
        IDUUIDriverInterface driver = _drivers.get(t.getCanonicalName().toString());
        if(driver  == null) {
            throw new InvalidParameterException(format("No driver %s in the composer installed!",t.getCanonicalName().toString()));
        }
        else {
            if(!driver.canAccept(object)) {
                throw new InvalidParameterException(format("The driver %s cannot accept %s as input!",t.getCanonicalName().toString(),object.getClass().getCanonicalName().toString()));
            }
        }
        _pipeline.add(object);
        return this;
    }

    public void run(JCas jc) throws IOException, InterruptedException, SAXException, TimeoutException {
        for(IDUUIPipelineComponent comp : _pipeline) {
            IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
            driver.instantiate(comp);
        }

        for(IDUUIPipelineComponent comp : _pipeline) {
            IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
            driver.run(comp,jc);
        }

        for(IDUUIPipelineComponent comp : _pipeline) {
            IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
            driver.destroy(comp);
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException, UIMAException, SAXException, TimeoutException {
        DUUIComposer composer = new DUUIComposer();
        DUUILocalDriver driver = new DUUILocalDriver();
        composer.addDriver(driver);

        composer.add(new DUUILocalDriver.Component("new:latest", true),DUUILocalDriver.class);

        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        composer.run(jc);

        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(),arr);
        System.out.printf("Result %s",arr.toString());
    }
}