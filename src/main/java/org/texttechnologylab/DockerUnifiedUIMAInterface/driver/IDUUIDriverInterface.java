package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface for all drivers
 *
 * @author Alexander Leonhardt
 */
public interface IDUUIDriverInterface {
    /**
     * Method for defining the Lua context to be used, which determines the transfer type between Composer and components.
     * @see DUUILuaContext
     * @param luaContext
     */
    public void setLuaContext(DUUILuaContext luaContext);

    /**
     * Method for checking whether the selected component can be used via the driver.
     * @param component
     * @return
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     */
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;

    /**
     * Initialisation method
     * @param component
     * @param jc
     * @param skipVerification
     * @param shutdown
     * @return
     * @throws Exception
     */
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception;

    /**
     * Visualisation of the concurrency
     * @param uuid
     */
    public void printConcurrencyGraph(String uuid);

    //TODO: public InputOutput get_inputs_and_outputs(String uuid)
    //Example: get_typesystem(...)

    /**
     * Returns the TypeSystem used for the respective component.
     * @see TypeSystemDescription
     * @param uuid
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     * @throws ResourceInitializationException
     */
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException;

    /**
     * Initializes a Reader Component
     * @param uuid
     * @param filePath
     * @return
     * @throws Exception
     */
    public int initReaderComponent(String uuid, Path filePath) throws Exception;

    /**
     * Starting a component.
     * @param uuid
     * @param aCas
     * @param perf
     * @param composer
     * @throws CASException
     * @throws PipelineComponentException
     */
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException;

    /**
     * Destruction of a component
     * @param uuid
     * @return
     */
    public boolean destroy(String uuid) throws IOException, InterruptedException;

    /**
     * Shutting down the driver
     */
    public void shutdown() throws IOException, InterruptedException;

}
