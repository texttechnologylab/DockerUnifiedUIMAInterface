package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.Timer;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import static java.lang.String.format;

public class DUUIReaderComposer extends DUUIComposer {
    /**
     * Composer constructor.
     *
     * @throws URISyntaxException
     */
    public DUUIReaderComposer() throws URISyntaxException {
        super();
    }
    /**
     * Instantiates the DUUI pipeline.
     * <p>
     * This setups, starts and checks every pipeline component and requests their UIMA typesystem to merge all types needed to process the full pipeline.
     * @return Merged typesystem based on all components
     * @throws Exception
     */

    public List<Integer> instantiateReaderPipeline(Path filePath) throws Exception {
        List<Integer> docCounts = new LinkedList<>();
        if (get_isServiceStarted())
            return null;

        Timer timer = new Timer();
        timer.start();

        set_hasShutdown(false);
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        if (get_skipVerification()) {
            addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    "Running without verification, no process calls will be made during initialization!");
        }

        // Reset "instantiated pipeline" as the components will duplicate otherwise
        // See https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/issues/34
        // TODO should this only be done in "resetPipeline"?
        //_instantiatedPipeline.clear();

        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(get_minimalTypesystem());
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        try {
            int index = 0;

            for (DUUIPipelineComponent comp : get_pipeline()) {
                if (shouldShutdown()) return null;

                IDUUIDriverInterface driver = get_drivers().get(comp.getDriver());
                getPipelineStatus().put(driver.getClass().getSimpleName(), DUUIStatus.INSTANTIATING);
                getPipelineStatus().put(comp.getName(), DUUIStatus.INSTANTIATING);

                // When a pipeline is run as a service, only components that are not yet instantiated
                // should be instantiated here.

                if (get_isServiceStarted() && get_instantiatedPipeline().size() > index) {
                    addEvent(
                            DUUIEvent.Sender.COMPOSER,
                            String.format("Reusing component %s", comp.getName())
                    );

                    TypeSystemDescription desc = driver.get_typesystem(get_instantiatedPipeline().get(index).getUUID());
                    Integer nDocs = driver.initReaderComponent(get_instantiatedPipeline().get(index).getUUID(), filePath);
                    if (desc != null) {
                        descriptions.add(desc);
                        docCounts.add(nDocs);
                    }
                } else {
                    addEvent(
                            DUUIEvent.Sender.COMPOSER,
                            String.format("Instantiating component %s", comp.getName())
                    );

                    String uuid = driver.instantiate(comp, jc, get_skipVerification(), get_shutdownAtomic());
                    if (uuid == null) {
                        shutdown();
                        return null;
                    }

                    DUUISegmentationStrategy segmentationStrategy = comp.getSegmentationStrategy();

                    TypeSystemDescription desc = driver.get_typesystem(uuid);
                    Integer nDocs = driver.initReaderComponent(uuid, filePath);
                    if (desc != null) {
                        descriptions.add(desc);
                        docCounts.add(nDocs);
                    }
                    //TODO: get input output of every annotator
                    get_instantiatedPipeline().add(new PipelinePart(driver, uuid, comp.getName(), segmentationStrategy));
                }

                index++;
                getPipelineStatus().put(comp.getName(), DUUIStatus.IDLE);
            }

            for (IDUUIDriverInterface driver : get_drivers().values()) {
                getPipelineStatus().put(driver.getClass().getSimpleName(), DUUIStatus.IDLE);
            }

            if (shouldShutdown()) return null;
            // UUID und die input outputs
            // Execution Graph
            // Gegeben Knoten n finde Vorgaenger
            // inputs: [], outputs: [Token]
            // input: [Sentences], outputs: [POS]
        } catch (InterruptedException e) {
            return null;
        } catch (Exception e) {
            addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    e.getMessage(),
                    DebugLevel.ERROR);

            throw e;
        }

        if (get_isServiceStarted() && getInstantiatedTypeSystem() != null) {
            addEvent(DUUIEvent.Sender.COMPOSER, "Reusing TypeSystemDescription");
        } else {
            setServiceStarted(isService());

            if (descriptions.size() > 1) {
                setInstantiatedTypeSystem(CasCreationUtils.mergeTypeSystems(descriptions));
            } else if (descriptions.size() == 1) {
                setInstantiatedTypeSystem(descriptions.get(0));
            } else {
                setInstantiatedTypeSystem(TypeSystemDescriptionFactory.createTypeSystemDescription());
            }
        }

        timer.stop();
        addEvent(
                DUUIEvent.Sender.COMPOSER,
                String.format("Instatiated Pipeline after %d ms.", timer.getDuration()));

        setInstantiationDuration(timer.getDuration());

        return docCounts;
    }

    @Override
    public DUUIReaderComposer withMonitor(DUUIMonitor monitor) throws UnknownHostException, InterruptedException {
        super.withMonitor(monitor);
        return this;
    }

    @Override
    public DUUIReaderComposer withSkipVerification(boolean skipVerification) {
        super.withSkipVerification(skipVerification);
        return this;
    }

    @Override
    public DUUIReaderComposer withStorageBackend(IDUUIStorageBackend storage) {
        super.withStorageBackend(storage);
        return this;
    }

    @Override
    public DUUIReaderComposer withLuaContext(DUUILuaContext context) {
        super.withLuaContext(context);
        return this;
    }

    @Override
    public DUUIReaderComposer withCasPoolsize(int poolsize) {
        super.withCasPoolsize(poolsize);
        return this;
    }

    @Override
    public DUUIReaderComposer withWorkers(int workers) {
        super.withWorkers(workers);
        return this;
    }

    @Override
    public DUUIReaderComposer withOpenConnection(boolean open) {
        super.withOpenConnection(open);
        return this;
    }

    @Override
    public DUUIReaderComposer addDriver(IDUUIDriverInterface driver) {
        super.addDriver(driver);
        return this;
    }

    @Override
    public DUUIReaderComposer addDriver(IDUUIDriverInterface... drivers) {
        super.addDriver(drivers);
        return this;
    }

    @Override
    public DUUIReaderComposer add(DUUIDockerDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    @Override
    public DUUIReaderComposer add(DUUIUIMADriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    @Override
    public DUUIReaderComposer add(DUUIRemoteDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    @Override
    public DUUIReaderComposer add(DUUISwarmDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    @Override
    public DUUIReaderComposer add(DUUIPipelineComponent object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        super.add(object);
        return this;
    }

    @Override
    public DUUIReaderComposer add(DUUIPipelineDescription desc) throws InvalidXMLException, IOException, SAXException, CompressorException {
        super.add(desc);
        return this;
    }

    @Override
    public DUUIReaderComposer resetPipeline() {
        super.resetPipeline();
        return this;
    }

    @Override
    public DUUIReaderComposer withInstantiatedPipeline(Vector<PipelinePart> pipeline) {
        super.withInstantiatedPipeline(pipeline);
        return this;
    }

    @Override
    public DUUIReaderComposer withIgnoreErrors(boolean ignoreErrors) {
        super.withIgnoreErrors(ignoreErrors);
        return this;
    }

    @Override
    public DUUIReaderComposer asService(boolean service) {
        super.asService(service);
        return this;
    }

    @Override
    public DUUIReaderComposer withDebugLevel(DebugLevel debugLevel) {
        super.withDebugLevel(debugLevel);
        return this;
    }
}
