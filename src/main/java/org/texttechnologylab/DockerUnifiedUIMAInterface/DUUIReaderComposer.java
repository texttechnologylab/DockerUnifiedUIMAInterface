package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyNone;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.Timer;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public List<Integer> instantiateReaderPipeline(Path filePath) throws Exception {
        List<Integer> docCounts = new LinkedList<>();
        try {
            int index = 0;
            for (DUUIPipelineComponent comp : get_pipeline()) {
                IDUUIDriverInterface driver = get_drivers().get(comp.getDriver());
                Integer nDocs = driver.initReaderComponent(get_instantiatedPipeline().get(index).getUUID(), filePath);
                docCounts.add(nDocs);
                index++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("DUUIReaderComposer instantiateReaderPipeline " + docCounts);
        return docCounts;
    }

    /*
    public List<Integer> instantiateReaderPipeline(Path filePath) throws Exception {
        List<Integer> docCounts = new LinkedList<>();
        try {
            int index = 0;
            for (DUUIPipelineComponent comp : get_pipeline()) {
                IDUUIDriverInterface driver = get_drivers().get(comp.getDriver());
                // When a pipeline is run as a service, only components that are not yet instantiated
                // should be instantiated here.
                System.out.println(get_isServiceStarted());
                System.out.println(get_instantiatedPipeline().size());
                System.out.println(index);
                if (get_instantiatedPipeline() != null && !get_instantiatedPipeline().isEmpty() && get_instantiatedPipeline().size() > index) {
                    Integer nDocs = driver.initReaderComponent(get_instantiatedPipeline().get(index).getUUID(), filePath);
                    docCounts.add(nDocs);

                } else {
                    JCas jc = JCasFactory.createJCas();
                    jc.setDocumentLanguage("en");
                    jc.setDocumentText("Hello World!");
                    String uuid = driver.instantiate(comp, jc, true, new AtomicBoolean(false));
                    Integer nDocs = driver.initReaderComponent(uuid, filePath);
                    docCounts.add(nDocs);
                }

                index++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return docCounts;
    }
    */

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

    /*
    @Override
    protected JCas run_pipeline(String name, JCas jc, long documentWaitTime, Vector<PipelinePart> pipeline) throws Exception {

        getProgressAtomic().set(0);

        if (name == null) {
            name = "UIMA-Document";
        }

        DUUIDocument document = null;
        if (jc.getDocumentText() == null) {
            document = new DUUIDocument(name, "/opt/path/");
        } else {
            document = new DUUIDocument(name, "/opt/path", jc.getDocumentText().getBytes(StandardCharsets.UTF_8));
        }

        if (JCasUtil.select(jc, DocumentMetaData.class).isEmpty()) {
            DocumentMetaData dmd = DocumentMetaData.create(jc);
            dmd.setDocumentId(document.getName());
            dmd.setDocumentTitle(document.getName());
            dmd.setDocumentUri(document.getPath());
            dmd.addToIndexes();
        }
        addDocument(document);

        boolean trackErrorDocs = false;
        if (get_storage() != null) {
            trackErrorDocs = get_storage().shouldTrackErrorDocs();
        }

        DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name, documentWaitTime, jc, trackErrorDocs);
        document.setStartedAt();
        document.setStatus(DUUIStatus.ACTIVE);

        Exception error = null;
        System.out.println("1");
        try {
            for (PipelinePart comp : pipeline) {
                if (shouldShutdown()) break;
                getPipelineStatus().put(comp.getName(), DUUIStatus.ACTIVE);

                // Segment document for each item in the pipeline separately
                // TODO support "complete pipeline" segmentation to only segment once
                DUUISegmentationStrategy segmentationStrategy = comp.getSegmentationStrategy();

                addEvent(
                        DUUIEvent.Sender.DOCUMENT,
                        String.format(
                                "%s is being processed by component %s",
                                document.getPath(),
                                comp.getName())
                );

                if (segmentationStrategy instanceof DUUISegmentationStrategyNone) {
                    System.out.println("2");
                    comp.getDriver().run(comp.getUUID(), jc, perf, this);
                    System.out.println("3");
                } else {
                    segmentationStrategy.initialize(jc);

                    JCas jCasSegmented = segmentationStrategy.getNextSegment();

                    while (jCasSegmented != null) {
                        // Process each cas sequentially
                        // TODO add parallel variant later

                        comp.getDriver().run(comp.getUUID(), jCasSegmented, perf, this);

                        segmentationStrategy.merge(jCasSegmented);
                        jCasSegmented = segmentationStrategy.getNextSegment();
                    }

                    segmentationStrategy.finalize(jc);
                }
                addEvent(
                        DUUIEvent.Sender.DOCUMENT,
                        String.format(
                                "%s has been processed by component %s",
                                document.getPath(),
                                comp.getName())
                );
                document.incrementProgress();
            }

            addEvent(
                    DUUIEvent.Sender.DOCUMENT,
                    String.format("%s has been processed",
                            document.getPath()));
            document.countAnnotations(jc);

        } catch (Exception exception) {
            error = exception;

            document.setError(String.format(
                    "%s%n%s",
                    exception.getClass().getCanonicalName(),
                    exception.getMessage() == null ? "" : exception.getMessage()));

            addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    exception.getMessage(),
                    DebugLevel.ERROR);

            // If we want to track errors we have to add the metrics for the document
            // TODO this should be configurable separately
            if (get_storage() == null) {
                throw exception;
            }
            if (!get_storage().shouldTrackErrorDocs()) {
                throw exception;
            }
        }

        if (error != null) {
            document.setStatus(DUUIStatus.FAILED);
        } else {
            document.setStatus(DUUIStatus.OUTPUT);
        }

        document.setFinishedAt();
        document.setFinished(true);

        if (get_storage() != null) {
            get_storage().addMetricsForDocument(perf);
        }

        incrementProgress();
        return jc;
    }
    */

}
