package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.InputsOutputs;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;

public interface IDUUIDriverInterface {
    public void setLuaContext(DUUILuaContext luaContext);
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception;
    public void printConcurrencyGraph(String uuid);

    public InputsOutputs getInputsOutputs();

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException;
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException, CASException;

    public void destroy(String uuid);
    public void shutdown();
}
