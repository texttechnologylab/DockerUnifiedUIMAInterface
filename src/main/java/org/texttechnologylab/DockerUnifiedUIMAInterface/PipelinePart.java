package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;
import org.xml.sax.SAXException;

public class PipelinePart {
    private final IDUUIDriver _driver;
    private final String _uuid;
    private final Signature _signature; 
    private final int _scale; 

    PipelinePart(IDUUIDriver driver, String uuid, int scale) {
        _driver = driver;
        _uuid = uuid;
        _signature = null;
        _scale = scale; 
    }

    public PipelinePart(IDUUIDriver driver, String uuid, Signature signature, int scale) {
        _driver = Objects.requireNonNull(driver);
        _uuid = Objects.requireNonNull(uuid);
        _signature = Objects.requireNonNull(signature);
        _scale = Objects.requireNonNull(scale); 
    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws AnalysisEngineProcessException, CASException, InterruptedException, IOException, SAXException, CompressorException {
        DUUIPipelineProfiler.add(name, _signature, _scale);
        _driver.run(_uuid, jc, perf);
    }

    public void shutdown() {
        System.out.printf("[Composer] Shutting down %s...\n", _uuid);
        _driver.destroy(_uuid);
    }

    public Signature getSignature() {
        return _signature; 
    }

    public IDUUIDriver getDriver() {
        return _driver;
    }

    public int getScale() {
        return _scale; 
    }

    public String getUUID() {
        return _uuid;
    }
}