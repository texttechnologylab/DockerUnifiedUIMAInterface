package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeoutException;

public interface IDUUIDriverInterface {
    public boolean canAccept(IDUUIPipelineComponent component);

    public String instantiate(IDUUIPipelineComponent component) throws Exception;

    public void printConcurrencyGraph(String uuid);

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException;
    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException;

    public void destroy(String uuid);
}
