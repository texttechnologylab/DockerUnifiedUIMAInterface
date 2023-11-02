package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

/**
 * Interface for all Drivers.
 */
public interface IDUUIDriver {

    /**
     * Set for Component UUID's to ensure uniqueness of UUID.
     */
    HashSet<String> _components = new HashSet<>(100); 

    /**
     * Set LuaContext used by the Driver.
     * 
     * @param luaContext
     */
    public default void setLuaContext(DUUILuaContext luaContext) {};

    /**
     * Test if a {@link DUUIPipelineComponent} belongs to a Driver.
     * 
     * @param component             Component to test.
     * @return                      True if Component belongs to this Driver, false otherwise.
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     */
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;

    /**
     * Instantiates a Component and makes necessary initial requests. 
     * 
     * @param component         Component to instantiate.
     * @param jc                Document used for verification-
     * @param skipVerification  Flag wether to perform verification.
     * @return                  UUID of Component.
     * @throws Exception
     */
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception;

    /**
     * Get typesystem of a Component necessary for the CAS documents.
     * 
     * @param uuid                              Component.
     * @return                                  Typesystem description used to instantiat CAS objects.
     * @throws ResourceInitializationException
     * @throws InterruptedException
     */
    public TypeSystemDescription get_typesystem(String uuid) throws ResourceInitializationException, InterruptedException;

    /**
     * Get inputs and outputs of a Component.
     * 
     * @param uuid                              Component.
     * @return                                  Signature containing inputs and outputs.
     * @throws ResourceInitializationException
     * @throws InterruptedException
     */
    public Signature get_signature(String uuid) throws ResourceInitializationException, InterruptedException;

    /**
     * Shutdown all instances of a Component and remove it from a pipeline.
     * 
     * @param uuid Component to remove.
     */
    public default void destroy(String uuid) {};

    /**
     * Any steps required to shutdown a Driver such as closing sockets or any other resources.
     * 
     */
    public default void shutdown() {}

    /**
     * Perform a process-call.
     * 
     * @param uuid  ID of Component used to process the document.
     * @param _jc   Document to process.
     * @param perf  Data-structure used to store performance metrics of the analysis.
     * @throws InterruptedException
     * @throws IOException
     * @throws SAXException
     * @throws AnalysisEngineProcessException
     * @throws CompressorException
     * @throws CASException
     */
    public void run(String uuid, JCas _jc, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException, CASException;
    
    public void printConcurrencyGraph(String uuid);

}
