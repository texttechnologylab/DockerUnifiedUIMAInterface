import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface IDUUIDriverInterface {
    public boolean canAccept(IDUUIPipelineComponent component);

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException, UIMAException, SAXException, IOException, CompressorException;

    public void printConcurrencyGraph(String uuid);

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException;

    public void destroy(String uuid);
}
