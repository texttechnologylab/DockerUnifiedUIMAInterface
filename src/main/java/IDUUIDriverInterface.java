import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public interface IDUUIDriverInterface {
    public boolean canAccept(IDUUIPipelineComponent component);
    public boolean instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException;
    public void run(IDUUIPipelineComponent component, JCas aCas) throws InterruptedException, IOException, SAXException;
    public void destroy(IDUUIPipelineComponent component);
}
