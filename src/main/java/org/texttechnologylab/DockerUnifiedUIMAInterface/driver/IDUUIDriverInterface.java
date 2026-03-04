package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
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
     *
     * @param luaContext
     * @see DUUILuaContext
     */
    void setLuaContext(DUUILuaContext luaContext);

    /**
     * Method for checking whether the selected component can be used via the driver.
     *
     * @param component
     * @return
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     */
    boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException;

    /**
     * Initialisation method
     *
     * @param component
     * @param jc
     * @param skipVerification
     * @param shutdown
     * @return
     * @throws Exception
     */
    String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception;

    /**
     * Visualisation of the concurrency
     *
     * @param uuid
     */
    void printConcurrencyGraph(String uuid);

    //TODO: public InputOutput get_inputs_and_outputs(String uuid)
    //Example: get_typesystem(...)

    /**
     * Returns the TypeSystem used for the respective component.
     *
     * @param uuid
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     * @throws ResourceInitializationException
     * @see TypeSystemDescription
     */
    TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException;

    /**
     * Initializes a Reader Component
     *
     * @param uuid
     * @param filePath
     * @return
     * @throws Exception
     */
    int initReaderComponent(String uuid, Path filePath) throws Exception;

    /**
     * Starting a component.
     *
     * @param uuid
     * @param aCas
     * @param perf
     * @param composer
     * @throws CASException
     * @throws PipelineComponentException
     */
    void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException;

    /**
     * Destruction of a component
     *
     * @param uuid
     * @return
     */
    boolean destroy(String uuid);

    /**
     * Shutting down the driver
     */
    void shutdown();

}
