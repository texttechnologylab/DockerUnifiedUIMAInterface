package org.texttechnology.DockerUnifiedUIMAInterface;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface IDUUIDriverInterface {
    public boolean canAccept(IDUUIPipelineComponent component);

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException, UIMAException, SAXException, IOException;

    public void printConcurrencyGraph(String uuid);

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException;

    public void destroy(String uuid);
}
