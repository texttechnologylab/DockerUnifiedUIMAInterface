package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

public class PipelinePart implements Serializable {
    private final IDUUIDriver _driver;
    private final String _uuid;
    private final Signature _signature; 
    private final DUUIPipelineComponent _component; 

    public PipelinePart(IDUUIDriver driver, String uuid, Signature signature, DUUIPipelineComponent component) {
        _driver = Objects.requireNonNull(driver);
        _uuid = Objects.requireNonNull(uuid);
        _signature = Objects.requireNonNull(signature);
        _component = Objects.requireNonNull(component); 
    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws AnalysisEngineProcessException, CASException, InterruptedException, IOException, SAXException, CompressorException {
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
        return _component.getScale(); 
    }

    public String getUUID() {
        return _uuid;
    }
}