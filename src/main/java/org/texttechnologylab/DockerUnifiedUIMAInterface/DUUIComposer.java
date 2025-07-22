package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.luaj.vm2.Globals;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.texttechnologylab.DockerUnifiedUIMAInterface.composer.DUUISegmentedWorker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionDBReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIEvent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyNone;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.Timer;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Worker thread processing a CAS following an execution plan.
 */
class DUUIWorker extends Thread {
    Vector<DUUIComposer.PipelinePart> _flow;
    ConcurrentLinkedQueue<JCas> _instancesToBeLoaded;
    ConcurrentLinkedQueue<JCas> _loadedInstances;
    AtomicInteger _threadsAlive;
    AtomicBoolean _shutdown;
    IDUUIStorageBackend _backend;
    String _runKey;
    AsyncCollectionReader _reader;
    IDUUIExecutionPlanGenerator _generator;

    DUUIComposer composer;

    /**
     * Worker constructor, only stores parameters.
     *
     * @param engineFlow Pipeline
     * @param emptyInstance CAS queue
     * @param loadedInstances CAS queue
     * @param shutdown Shutdown indicator
     * @param error Signal if thread is still active
     * @param backend Storage backend used for statistics and error reporting
     * @param runKey Key identifying this run
     * @param reader CAS collection reader
     * @param generator Execution plan generator
     * @param composer Reference to the composer instance
     */
    DUUIWorker(Vector<DUUIComposer.PipelinePart> engineFlow, ConcurrentLinkedQueue<JCas> emptyInstance, ConcurrentLinkedQueue<JCas> loadedInstances, AtomicBoolean shutdown, AtomicInteger error,
               IDUUIStorageBackend backend, String runKey, AsyncCollectionReader reader, IDUUIExecutionPlanGenerator generator, DUUIComposer composer) {
        super();
        _flow = engineFlow;
        _instancesToBeLoaded = emptyInstance;
        _loadedInstances = loadedInstances;
        _shutdown = shutdown;
        _threadsAlive = error;
        _backend = backend;
        _runKey = runKey;
        _reader = reader;
        _generator = generator;
        this.composer = composer;
    }

    /**
     * Runs the DUUI worker as a thread.
     * <p>
     * This processes all CAS objects based on an execution plan.
     */
    @Override
    public void run() {
        int num = _threadsAlive.addAndGet(1);
        while (true) {
            JCas object = null;
            long waitTimeStart = System.nanoTime();
            long waitTimeEnd = 0;
            while (object == null) {
                object = _loadedInstances.poll();

                if (_shutdown.get() && object == null) {
                    _threadsAlive.getAndDecrement();
                    return;
                }

                if (object == null && _reader != null) {
                    object = _instancesToBeLoaded.poll();
                    if (object == null)
                        continue;
                    try {
                        waitTimeEnd = System.nanoTime();
                        if (!_reader.getNextCAS(object)) {
                            _threadsAlive.getAndDecrement();
                            _instancesToBeLoaded.add(object);
                            //Give the main IO Thread time to finish work
                            Thread.sleep(300);
                            object = null;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (CompressorException e) {
                        e.printStackTrace();
                    } catch (SAXException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (NullPointerException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (waitTimeEnd == 0) waitTimeEnd = System.nanoTime();
            IDUUIExecutionPlan execPlan = _generator.generate(object);

            //System.out.printf("[Composer] Thread %d still alive and doing work\n",num);

            boolean trackErrorDocs = false;
            if (_backend != null) {
                trackErrorDocs = _backend.shouldTrackErrorDocs();
            }

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(_runKey,
                waitTimeEnd - waitTimeStart,
                object,
                trackErrorDocs);
            // f32, 64d, e57
            // DAG, Directed Acyclic Graph
            boolean done = false;
            List<Future<IDUUIExecutionPlan>> pendingFutures = new LinkedList<>();
            // await entry
            pendingFutures.add(execPlan.awaitMerge());
            //pendingFutures = [exec(entry)]

            while (!pendingFutures.isEmpty()) {
                List<Future<IDUUIExecutionPlan>> newFutures = new LinkedList<>();
                pendingFutures.removeIf(pending -> {
                    if (pending.isDone()) {
                        IDUUIExecutionPlan mergedPlan = null;
                        try {
                            mergedPlan = pending.get();
                            //0: exec(entry)
                            //1: exec(a)
                            //2: exec(b)
                            //3: exec(c)

                            DUUIComposer.PipelinePart i = mergedPlan.getPipelinePart();
                            if (i != null) {
                                i.getDriver().run(i.getUUID(), mergedPlan.getJCas(), perf, composer);
                            }
                            //0: a,b,c
                            //1: exec(a) : d
                            //2: exec(b) : d
                            //3: exec(c) : d
                            for (IDUUIExecutionPlan plan : mergedPlan.getNextExecutionPlans()) {
                                //0: newFutures = [fut(exec(a)), fut(exec(b)), future(exec(c))]
                                newFutures.add(plan.awaitMerge());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return true;
                    }
                    return false;
                });
                pendingFutures.addAll(newFutures);
                //0: pendingFutures = [fut(exec(a)), fut(exec(b)), future(exec(c))]
                //4: pendingFutures = [fut(exec(d)), fut(exec(d)), fut(exec(d))]
            }

            object.reset();
            _instancesToBeLoaded.add(object);
            if (_backend != null) {
                _backend.addMetricsForDocument(perf);
            }
        }
    }
}

/**
 * DUUI worker thread that processes CAS objects using a defined pipeline of components.
 */
class DUUIWorkerAsyncReader extends Thread {
    Vector<DUUIComposer.PipelinePart> _flow;
    AtomicInteger _threadsAlive;
    AtomicBoolean _shutdown;
    IDUUIStorageBackend _backend;
    JCas _jc;
    String _runKey;
    AsyncCollectionReader _reader;
    DUUIComposer composer;

    /**
     * Worker constructor, only stores parameters.
     *
     * @param engineFlow Pipeline
     * @param jc Current CAS to process
     * @param shutdown Shutdown indicator
     * @param error Signal if thread is still active
     * @param backend Storage backend used for statistics and error reporting
     * @param runKey Key identifying this run
     * @param reader CAS collection reader
     * @param composer Reference to the composer instance
     */
    DUUIWorkerAsyncReader(Vector<DUUIComposer.PipelinePart> engineFlow, JCas jc, AtomicBoolean shutdown, AtomicInteger error,
                          IDUUIStorageBackend backend, String runKey, AsyncCollectionReader reader, DUUIComposer composer) {
        super();
        _flow = engineFlow;
        _jc = jc;
        _shutdown = shutdown;
        _threadsAlive = error;
        _backend = backend;
        _runKey = runKey;
        _reader = reader;
        this.composer = composer;
    }

    /**
     * Runs the pipeline processing CAS.
     */
    @Override
    public void run() {
        int num = _threadsAlive.addAndGet(1);
        while (true) {
            long waitTimeStart = System.nanoTime();
            long waitTimeEnd = 0;
            while (true) {
                if (_shutdown.get()) {
                    _threadsAlive.getAndDecrement();
                    return;
                }
                try {
                    if (!_reader.getNextCAS(_jc)) {
                        //Give the main IO Thread time to finish work
                        Thread.sleep(300);
                    } else {
                        break;
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (CompressorException e) {
                    e.printStackTrace();
                } catch (SAXException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (waitTimeEnd == 0) waitTimeEnd = System.nanoTime();

            //System.out.printf("[Composer] Thread %d still alive and doing work\n",num);

            boolean trackErrorDocs = false;
            if (_backend != null) {
                trackErrorDocs = _backend.shouldTrackErrorDocs();
            }

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(_runKey,
                waitTimeEnd - waitTimeStart,
                _jc,
                trackErrorDocs);
            for (DUUIComposer.PipelinePart i : _flow) {
                try {
                    // Segment document for each item in the pipeline separately
                    // TODO support "complete pipeline" segmentation to only segment once
                    // TODO thread safety needed for here?
                    DUUISegmentationStrategy segmentationStrategy = i.getSegmentationStrategy();
                    if (segmentationStrategy instanceof DUUISegmentationStrategyNone) {
                        i.getDriver().run(i.getUUID(), _jc, perf, composer);
                    } else {
                        segmentationStrategy.initialize(_jc);

                        JCas jCasSegmented = segmentationStrategy.getNextSegment();

                        while (jCasSegmented != null) {
                            // Process each cas sequentially
                            // TODO add parallel variant later

                            if (segmentationStrategy instanceof DUUISegmentationStrategyByDelemiter) {
                                DUUISegmentationStrategyByDelemiter pStrategie = ((DUUISegmentationStrategyByDelemiter) segmentationStrategy);

                                if (pStrategie.hasDebug()) {
                                    int iLeft = pStrategie.getSegments();
                                    DocumentMetaData dmd = DocumentMetaData.get(_jc);
                                    System.out.println(dmd.getDocumentId() + " Left: " + iLeft);
                                }


                            }
                            i.getDriver().run(i.getUUID(), jCasSegmented, perf, composer);

                            segmentationStrategy.merge(jCasSegmented);

                            jCasSegmented = segmentationStrategy.getNextSegment();
                        }

                        segmentationStrategy.finalize(_jc);
                    }

                } catch (Exception e) {
                    //Ignore errors at the moment
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    System.out.println("Thread continues work with next document!");
                    break;
                }
            }

            if (_backend != null) {
                _backend.addMetricsForDocument(perf);
            }
        }
    }
}

/**
 * DUUI worker for processing a pipeline based on async reader processor.
 */
class DUUIWorkerAsyncProcessor extends Thread {
    Vector<DUUIComposer.PipelinePart> _flow;
    AtomicInteger _threadsAlive;
    AtomicBoolean _shutdown;
    IDUUIStorageBackend _backend;
    JCas _jc;
    String _runKey;
    DUUIAsynchronousProcessor _processor;
    DUUIComposer composer;

    /**
     * Worker constructor, only stores parameters.
     *
     * @param engineFlow Pipeline
     * @param jc Current CAS to process
     * @param shutdown Shutdown indicator
     * @param error Signal if thread is still active
     * @param backend Storage backend used for statistics and error reporting
     * @param runKey Key identifying this run
     * @param processor CAS async reader
     * @param composer Reference to the composer instance
     */
    DUUIWorkerAsyncProcessor(Vector<DUUIComposer.PipelinePart> engineFlow, JCas jc, AtomicBoolean shutdown, AtomicInteger error,
                             IDUUIStorageBackend backend, String runKey, DUUIAsynchronousProcessor processor, DUUIComposer composer) {
        super();
        _flow = engineFlow;
        _jc = jc;
        _shutdown = shutdown;
        _threadsAlive = error;
        _backend = backend;
        _runKey = runKey;
        _processor = processor;
        this.composer = composer;
    }

    /**
     * Runs the pipeline to process CAS.
     */
    @Override
    public void run() {
        int num = _threadsAlive.addAndGet(1);
        do {
            long waitTimeStart = System.nanoTime();
            long waitTimeEnd = 0;
            while (true) {
                if (_shutdown.get()) {
                    _threadsAlive.getAndDecrement();
                    return;
                }
                try {
                    _jc.reset();
                    if (!_processor.getNextCAS(_jc)) {
                        //Give the main IO Thread time to finish work
                        Thread.sleep(300);
                    } else {
                        break;
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (waitTimeEnd == 0) waitTimeEnd = System.nanoTime();

            //System.out.printf("[Composer] Thread %d still alive and doing work\n",num);

            boolean trackErrorDocs = false;
            if (_backend != null) {
                trackErrorDocs = _backend.shouldTrackErrorDocs();
            }

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(_runKey,
                waitTimeEnd - waitTimeStart,
                _jc,
                trackErrorDocs);
            for (DUUIComposer.PipelinePart i : _flow) {
                try {
                    // Segment document for each item in the pipeline separately
                    // TODO support "complete pipeline" segmentation to only segment once
                    // TODO thread safety needed for here?
                    DUUISegmentationStrategy segmentationStrategy = i.getSegmentationStrategy();
                    if (segmentationStrategy instanceof DUUISegmentationStrategyNone) {

                        i.getDriver().run(i.getUUID(), _jc, perf, composer);
                    } else {
                        segmentationStrategy.initialize(_jc);

                        JCas jCasSegmented = segmentationStrategy.getNextSegment();

                        while (jCasSegmented != null) {
                            // Process each cas sequentially
                            // TODO add parallel variant later

                            if (segmentationStrategy instanceof DUUISegmentationStrategyByDelemiter) {
                                DUUISegmentationStrategyByDelemiter pStrategie = ((DUUISegmentationStrategyByDelemiter) segmentationStrategy);

                                if (pStrategie.hasDebug()) {
                                    int iLeft = pStrategie.getSegments();
                                    DocumentMetaData dmd = DocumentMetaData.get(_jc);
                                    System.out.println(dmd.getDocumentId() + " Left: " + iLeft);
                                }


                            }
                            i.getDriver().run(i.getUUID(), jCasSegmented, perf, composer);

                            segmentationStrategy.merge(jCasSegmented);

                            jCasSegmented = segmentationStrategy.getNextSegment();
                        }

                        segmentationStrategy.finalize(_jc);
                    }

                } catch (Exception e) {
                    //Ignore errors at the moment
                    e.printStackTrace();
                    if (!(e instanceof IOException)) {
                        System.err.println(e.getMessage());
                        e.printStackTrace();
                        System.out.println("Thread continues work with next document!");
                        break;
                    }
                }

            }

            if (_backend != null) {
                _backend.addMetricsForDocument(perf);
            }
        }
        while (!_processor.isFinish());

    }
}

/**
 * DUUI worker based on document reader
 */
class DUUIWorkerDocumentReader extends Thread {
    Vector<DUUIComposer.PipelinePart> flow;
    AtomicInteger threadsAlive;
    AtomicBoolean shutdown;
    IDUUIStorageBackend backend;
    JCas cas;
    String runKey;
    DUUIDocumentReader reader;
    DUUIComposer composer;

    /**
     * Worker constructor, only stores parameters.
     *
     * @param flow Pipeline
     * @param cas Current CAS to process
     * @param shutdown Shutdown indicator
     * @param threadsAlive Signal if thread is still active
     * @param backend Storage backend used for statistics and error reporting
     * @param runKey Key identifying this run
     * @param reader CAS collection reader
     * @param composer Reference to the composer instance
     */
    DUUIWorkerDocumentReader(
        Vector<DUUIComposer.PipelinePart> flow,
        JCas cas,
        AtomicBoolean shutdown,
        AtomicInteger threadsAlive,
        IDUUIStorageBackend backend,
        String runKey,
        DUUIDocumentReader reader,
        DUUIComposer composer
    ) {
        super();

        this.flow = flow;
        this.cas = cas;
        this.shutdown = shutdown;
        this.threadsAlive = threadsAlive;
        this.backend = backend;
        this.runKey = runKey;
        this.reader = reader;
        this.composer = composer;
    }

    /**
     * Runs the pipeline.
     */
    @Override
    public void run() {

        composer.addEvent(
            DUUIEvent.Sender.COMPOSER,
            String.format("%d threads are active.", threadsAlive.addAndGet(1))
        );

        DUUIDocument document;

        while (!composer.shouldShutdown()) {
            Timer timer = new Timer();
            timer.start();

            while (true) {
                if (composer.shouldShutdown()) {
                    threadsAlive.getAndDecrement();
                    return;
                }

                try {
                    document = reader.getNextDocument(cas);
                    if (document != null && !document.isFinished()) break;

                    Thread.sleep(300);
                } catch (IllegalArgumentException ignored) {
                } catch (InterruptedException e) {
                    composer.addEvent(
                        DUUIEvent.Sender.COMPOSER,
                        e.getMessage(),
                        DUUIComposer.DebugLevel.ERROR
                    );
                }
            }

            timer.stop();

            boolean trackErrorDocs = false;
            if (backend != null) {
                trackErrorDocs = backend.shouldTrackErrorDocs();
            }

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(runKey,
                timer.getDuration(),
                cas,
                trackErrorDocs);

            document.setDurationWait(timer.getDuration());
            composer.addEvent(
                DUUIEvent.Sender.DOCUMENT,
                String.format("Starting to process %s", document.getPath()));

            timer.restart();

            document.setStatus(DUUIStatus.ACTIVE);

            for (DUUIComposer.PipelinePart pipelinePart : flow) {
                composer.addEvent(
                    DUUIEvent.Sender.DOCUMENT,
                    String.format(
                        "%s is being processed by component %s",
                        document.getPath(),
                        pipelinePart.getName())
                );

                try {
                    DUUISegmentationStrategy segmentationStrategy = pipelinePart.getSegmentationStrategy();
                    if (segmentationStrategy instanceof DUUISegmentationStrategyNone) {
                        composer.setPipelineStatus(
                            pipelinePart.getName(),
                            DUUIStatus.ACTIVE);

                        composer.setPipelineStatus(
                            pipelinePart.getDriver().getClass().getSimpleName(),
                            DUUIStatus.ACTIVE);


                        pipelinePart.getDriver().run(pipelinePart.getUUID(), cas, perf, composer);
                    } else {
                        segmentationStrategy.initialize(cas);
                        JCas jCasSegmented = segmentationStrategy.getNextSegment();

                        while (jCasSegmented != null) {
                            if (segmentationStrategy instanceof DUUISegmentationStrategyByDelemiter) {
                                DUUISegmentationStrategyByDelemiter pStrategie = ((DUUISegmentationStrategyByDelemiter) segmentationStrategy);

                                if (pStrategie.hasDebug()) {
                                    int iLeft = pStrategie.getSegments();
                                    DocumentMetaData dmd = DocumentMetaData.get(cas);
                                    composer.addEvent(
                                        DUUIEvent.Sender.COMPOSER,
                                        String.format("%s Left: %s", dmd.getDocumentId(), iLeft)
                                    );
                                }
                            }
                            pipelinePart.getDriver().run(pipelinePart.getUUID(), jCasSegmented, perf, composer);
                            segmentationStrategy.merge(jCasSegmented);
                            jCasSegmented = segmentationStrategy.getNextSegment();
                        }

                        segmentationStrategy.finalize(cas);
                    }

                } catch (AnalysisEngineProcessException exception) {
                    composer.setPipelineStatus(pipelinePart.getName(), DUUIStatus.FAILED);

                    composer.addEvent(
                        DUUIEvent.Sender.DOCUMENT,
                        String.format(
                            "%s encountered error %s. Thread continues work with next document.",
                            document.getPath(), exception.getMessage()));

                    document.setError(String.format(
                        "%s%n%s",
                        exception.getClass().getCanonicalName(),
                        exception.getMessage() == null ? "" : exception.getMessage()));

                    document.setStatus(DUUIStatus.FAILED);

                    if (composer.getIgnoreErrors()) {
                        break;
                    } else {
                        throw new RuntimeException(exception);
                    }
                } catch (Exception exception) {
                    composer.addEvent(
                        DUUIEvent.Sender.DOCUMENT,
                        String.format(
                            "%s encountered error %s. Thread continues work with next document.",
                            document.getPath(), exception));

                    document.setError(String.format(
                        "%s%n%s",
                        exception.getClass().getCanonicalName(),
                        exception.getMessage() == null ? "" : exception.getMessage()));
                    document.setStatus(DUUIStatus.FAILED);

                    if (composer.getIgnoreErrors()) {
                        break;
                    } else {
                        throw new RuntimeException(exception.getMessage());
                    }
                }
                composer.addEvent(
                    DUUIEvent.Sender.DOCUMENT,
                    String.format(
                        "%s has been processed by component %s",
                        document.getPath(),
                        pipelinePart.getName())
                );
                document.incrementProgress();
            }


            timer.stop();

            if (!document.getStatus().equals(DUUIStatus.FAILED)) {
                document.setStatus(reader.hasOutput() ? DUUIStatus.OUTPUT : DUUIStatus.COMPLETED);
                document.countAnnotations(cas);

                if (reader.hasOutput()) {
                    try {
                        reader.upload(document, cas);
                    } catch (IOException | SAXException exception) {
                        document.setError(String.format(
                            "%s%n%s",
                            exception.getClass().getCanonicalName(),
                            exception.getMessage() == null ? "" : exception.getMessage()));
                        document.setStatus(DUUIStatus.FAILED);

                        if (!composer.getIgnoreErrors())
                            throw new RuntimeException(exception);
                    }
                }
            }

            composer.addEvent(
                DUUIEvent.Sender.DOCUMENT,
                String.format("%s has been processed after %d ms",
                    document.getPath(),
                    timer.getDuration()));

            if (backend != null) {
                backend.addMetricsForDocument(perf);
            }

            composer.incrementProgress();
            document.setDurationProcess(timer.getDuration());
            document.setFinished(true);
            document.setFinishedAt();
        }
    }

}

/**
 * DUUI composer.
 * <p>
 * This is the main class used to control and use DUUI.
 */
public class DUUIComposer {
    private final Map<String, IDUUIDriverInterface> _drivers;
    private final Vector<DUUIPipelineComponent> _pipeline;
    private int _workers;
    public Integer _cas_poolsize;
    private DUUILuaContext _context;
    private DUUIMonitor _monitor;
    private IDUUIStorageBackend _storage;
    private boolean _skipVerification;
    private Vector<PipelinePart> _instantiatedPipeline;
    private Thread _shutdownHook;
    private AtomicBoolean _shutdownAtomic;
    private boolean _hasShutdown;

    private static final String DRIVER_OPTION_NAME = "duuid.composer.driver";
    public static final String COMPONENT_COMPONENT_UNIQUE_KEY = "duuid.storage.componentkey";

    public static final String V1_COMPONENT_ENDPOINT_PROCESS = "/v1/process";
    public static final String V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET = "/v1/process_websocket";
    public static final String V1_COMPONENT_ENDPOINT_TYPESYSTEM = "/v1/typesystem";
    public static final String V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER = "/v1/communication_layer";

    public static List<IDUUIConnectionHandler> _clients = new ArrayList<>(); // Saves Websocket-Clients.
    private boolean _connection_open = false; // Let connection open for multiple consecutive use.

    public enum DebugLevel {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR,
        CRITICAL,
        NONE;
    }

    private TypeSystemDescription _minimalTypesystem;
    private TypeSystemDescription instantiatedTypeSystem;
    private List<DUUIEvent> events = new ArrayList<>();
    private Map<String, DUUIDocument> documents = new HashMap<>();
    private Map<String, String> pipelineStatus = new HashMap<>();
    private DebugLevel debugLevel = DebugLevel.NONE;
    private boolean ignoreErrors = false;
    private boolean isService = false;
    private boolean isServiceStarted = false;
    private AtomicBoolean isFinished = new AtomicBoolean(false);
    private long instantiationDuration;
    private AtomicInteger progress = new AtomicInteger(0);

    /**
     * Composer constructor.
     * @throws URISyntaxException
     */
    public DUUIComposer() throws URISyntaxException {
        _drivers = new HashMap<>();
        _pipeline = new Vector<>();
        _workers = 1;
        _cas_poolsize = null;
        Globals globals = JsePlatform.standardGlobals();
        _context = new DUUILuaContext();
        _monitor = null;
        _storage = null;
        _skipVerification = false;
        _hasShutdown = false;
        _shutdownAtomic = new AtomicBoolean(false);
        _instantiatedPipeline = new Vector<>();
        _minimalTypesystem = TypeSystemDescriptionFactory
            .createTypeSystemDescriptionFromPath(
                Objects.requireNonNull(
                        DUUIComposer
                            .class
                            .getClassLoader()
                            .getResource("org/texttechnologylab/types/reproducibleAnnotations.xml")
                    ).toURI()
                    .toString());

        addEvent(
            DUUIEvent.Sender.COMPOSER,
            String.format("[Composer] Initialised LUA scripting layer with version %s", globals.get("_VERSION")),
            DebugLevel.INFO);

        DUUIComposer that = this;
        _shutdownHook = new Thread(() -> {
            try {
                that.shutdown();
                addEvent(DUUIEvent.Sender.COMPOSER, "Shutdown Hook finished.");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Runtime.getRuntime().addShutdownHook(_shutdownHook);
    }

    public static String getLocalhost() {
        return "http://127.0.0.1";
    }

    /**
     * Attach InfluxDB for monitoring.
     * @param monitor DUUI monitor object
     * @return this, for method chaining
     * @throws UnknownHostException
     * @throws InterruptedException
     */
    public DUUIComposer withMonitor(DUUIMonitor monitor) throws UnknownHostException, InterruptedException {
        _monitor = monitor;
        _monitor.setup();
        return this;
    }

    /**
     * Enable or disable DUUI API verification step, by default verification is enabled.
     * <p>
     * If enabled, DUUI tries to process sample data via every component in the pipeline to check for DUUI API compatibility before processing the actual CAS objects.
     * @param skipVerification true for skipping, else false
     * @return this, for method chaining
     */
    public DUUIComposer withSkipVerification(boolean skipVerification) {
        _skipVerification = skipVerification;
        return this;
    }

    /**
     * Attach a storage backend to collect metrics and errors of a run in a database.
     * @param storage Storage backend, e.g. SQLite.
     * @return this, for method chaining
     */
    public DUUIComposer withStorageBackend(IDUUIStorageBackend storage) {
        _storage = storage;
        return this;
    }

    /**
     * Set Lua context to use.
     * <p>
     * This enables the configuration of sandbox features or globally usable libraries for Lua scripts.
     * @param context Lua context
     * @return this, for method chaining
     */
    public DUUIComposer withLuaContext(DUUILuaContext context) {
        _context = context;
        return this;
    }

    /**
     * Set CAS poolsize.
     * <p>
     * This is calculated by default (or if explicitly set to null) based on amount of workers.
     * @param poolsize CAS poolsize
     * @return this, for method chaining
     */
    public DUUIComposer withCasPoolsize(int poolsize) {
        _cas_poolsize = poolsize;
        return this;
    }

    /**
     * Set the amount of DUUI worker threads for processing, defaults to 1 if not set.
     * @param workers Amount of workers to use.
     * @return this, for method chaining
     */
    public DUUIComposer withWorkers(int workers) {
        _workers = workers;
        return this;
    }

    /**
     *
     * @param open
     * @return this, for method chaining
     */
    public DUUIComposer withOpenConnection(boolean open) {
        _connection_open = open;
        return this;
    }

    /**
     * Register a driver to use with the DUUI composer.
     * <p>
     * By default, no driver is setup. To use the composer, at least one driver has to be added. Components depending on drivers that have not been added can not be processed.
     * @see IDUUIDriverInterface
     * @param driver The driver to register.
     * @return this, for method chaining
     */
    public DUUIComposer addDriver(IDUUIDriverInterface driver) {
        driver.setLuaContext(_context);
        _drivers.put(driver.getClass().getCanonicalName(), driver);
        return this;
    }

    /**
     * Register multiple drivers to use with the DUUI controller.
     * <p>
     * By default, no driver is setup. To use the composer, at least one driver has to be added. Components depending on drivers that have not been added can not be processed.
     * @see IDUUIDriverInterface
     * @param drivers The drivers to register.
     * @return this, for method chaining
     */
    public DUUIComposer addDriver(IDUUIDriverInterface... drivers) {
        for (IDUUIDriverInterface driver : drivers) {
            driver.setLuaContext(_context);
            _drivers.put(driver.getClass().getCanonicalName(), driver);
        }
        return this;
    }

   /*public IDUUIPipelineComponent addFromBackend(String id) {
        if(_storage == null) {
            throw new RuntimeException("[DUUIComposer] No storage backend specified but trying to load component from it!");
        }
        _pipeline.add(_storage.loadComponent(id));
        IDUUIDriverInterface driver = _drivers.get(_pipeline.lastElement().getOption(DUUIComposer.DRIVER_OPTION_NAME));
        if (driver == null) {
            throw new InvalidParameterException(format("[DUUIComposer] No driver %s in the composer installed!", _pipeline.lastElement().getOption(DUUIComposer.DRIVER_OPTION_NAME)));
        }
        return _pipeline.lastElement();
    }*/

    /**
     * Adds a Docker component to the pipeline.
     * @param object Docker component
     * @return this, for method chaining
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     */
    public DUUIComposer add(DUUIDockerDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    /**
     * Adds a UIMA component to the pipeline.
     * @param object UIMA component
     * @return this, for method chaining
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     */
    public DUUIComposer add(DUUIUIMADriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    /**
     * Adds a Docker component to the pipeline.
     * @param object Docker component
     * @return this, for method chaining
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     */
    public DUUIComposer add(DUUIRemoteDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    /**
     * Adds a Docker Swarm component to the pipeline.
     * @param object Docker Swarm component
     * @return this, for method chaining
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     */
    public DUUIComposer add(DUUISwarmDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    /**
     * Adds a component to the pipeline.
     * @param object component
     * @return this, for method chaining
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     */
    public DUUIComposer add(DUUIPipelineComponent object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        IDUUIDriverInterface driver = _drivers.get(object.getDriver());
        if (driver == null) {
            throw new InvalidParameterException(format("[DUUIComposer] No driver %s in the composer installed!", object.getDriver()));
        } else {
            if (!driver.canAccept(object)) {
                throw new InvalidParameterException(format("[DUUIComposer] The driver %s cannot accept %s as input!", object.getDriver(), object.getClass().getCanonicalName()));
            }
        }

        System.out.println("[DUUIComposer] Compressing and finalizing pipeline component...");
        object.finalizeComponent();
        _pipeline.add(object);
        return this;
    }

    /**
     * Adds a component to the pipeline.
     * @param desc component
     * @return this, for method chaining
     * @throws InvalidXMLException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     */
    public DUUIComposer add(DUUIPipelineDescription desc) throws InvalidXMLException, IOException, SAXException, CompressorException {
        for (DUUIPipelineAnnotationComponent ann : desc.getComponents()) {
            add(ann.getComponent());
        }
        return this;
    }

    /**
     * Represents a tool in the pipeline.
     */
    public static class PipelinePart {
        private final IDUUIDriverInterface _driver;
        private final String _uuid;
        private final String name;
        private final DUUISegmentationStrategy segmentationStrategy;

        /**
         * Construct pipeline part.
         * @param driver DUUI driver to use
         * @param uuid Unique ID of this part
         * @param name Part name
         * @param segmentationStrategy Segmentation strategy to use
         */
        PipelinePart(IDUUIDriverInterface driver, String uuid, String name, DUUISegmentationStrategy segmentationStrategy) {
            _driver = driver;
            _uuid = uuid;
            this.name = name;
            this.segmentationStrategy = segmentationStrategy;
        }

        public IDUUIDriverInterface getDriver() {
            return _driver;
        }

        public String getUUID() {
            return _uuid;
        }

        public String getName() {
            return name;
        }

        public DUUISegmentationStrategy getSegmentationStrategy() {
            if (segmentationStrategy == null) {
                // Use default strategy with no segmentation
                return new DUUISegmentationStrategyNone();
            }

            // Always return a copy to allow for multiple processes/threads
//            System.out.println("Cloning segmentation strategy: " + segmentationStrategy.getClass().getName());
            return SerializationUtils.clone(segmentationStrategy);
        }
    }

    /**
     * Resets the DUUI pipeline to prepare a new run.
     * @return this, for method chaining
     */
    public DUUIComposer resetPipeline() {
        events.clear();
        _pipeline.clear();
        documents.clear();
        progress.set(0);
        isServiceStarted = false;
        return this;
    }

    /**
     * Run the composer pipeline.
     * @param collectionReader CAS reader based on an async processor
     * @param name Name for this run.
     * @throws Exception
     */
    public void run(DUUIAsynchronousProcessor collectionReader, String name) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        _shutdownAtomic.set(false);

        Exception catched = null;

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);

        try {
            if (_storage != null) {
                _storage.addNewRun(name, this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            if (_cas_poolsize == null) {
                _cas_poolsize = (int) Math.ceil(_workers * 1.5);
                System.out.printf("[Composer] Calculated CAS poolsize of %d!\n", _cas_poolsize);
            } else {
                if (_cas_poolsize < _workers) {
                    System.err.println("[Composer] WARNING: Pool size is smaller than the available threads, this is likely a bottleneck.");
                }
            }

            for (int i = 0; i < _cas_poolsize; i++) {
                emptyCasDocuments.add(JCasFactory.createJCas(desc));
            }

            Thread[] arr = new Thread[_workers];
            for (int i = 0; i < _workers; i++) {
                System.out.printf("[Composer] Starting worker thread [%d/%d]\n", i + 1, _workers);
                arr[i] = new DUUIWorkerAsyncProcessor(_instantiatedPipeline, emptyCasDocuments.poll(), _shutdownAtomic, aliveThreads, _storage, name, collectionReader, this);
                arr[i].start();
            }
            Instant starttime = Instant.now();
//            while (!_shutdownAtomic.get()) {
//
//            }

            AtomicInteger waitCount = new AtomicInteger();
            waitCount.set(0);
            // Wartet, bis die Dokumente fertig verarbeitet wurden.
            while (emptyCasDocuments.size() != _cas_poolsize && !collectionReader.isFinish()) {
                if (waitCount.incrementAndGet() % 500 == 0) {
                    System.out.println("[Composer] Waiting for threads to finish document processing...");
                }
                Thread.sleep(1000 * _workers); // to fast or in relation with threads?

            }
            System.out.println("[Composer] All documents have been processed. Signaling threads to shut down now...");
            _shutdownAtomic.set(true);

            for (int i = 0; i < arr.length; i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n", i + 1, arr.length);
                arr[i].join();
                System.out.printf("[Composer] Thread %d returned.\n", i);
            }
            if (_storage != null) {
                _storage.finalizeRun(name, starttime, Instant.now());
            }
            System.out.println("[Composer] All threads returned.");
            shutdown_pipeline();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            shutdown_pipeline();
            throw e;
        }
    }

    /**
     * Run the pipeline.
     * <p>
     * This runs the pipeline by segmenting all CAS using a database-backed collection reader and segment storage.
     * @param collectionReader Collection reader providing segmentation and merge functionality.
     * @param name Run name.
     * @throws Exception
     */
    public void runSegmented(DUUICollectionDBReader collectionReader, String name) throws Exception {
        try {
            _shutdownAtomic.set(false);

            if(_storage != null) {
                _storage.addNewRun(name, this);
            }

            TypeSystemDescription desc = instantiate_pipeline();

            List<String> pipelineUUIDs = _instantiatedPipeline.stream().map(PipelinePart::getUUID).collect(Collectors.toList());

            int threadsPerTool = _workers / _instantiatedPipeline.size();
            System.out.printf("[Composer] Running in segmented mode, %d threads with %d threads per tool!\n", _workers, threadsPerTool);

            List<Thread> threads = new ArrayList<>();
            int tId = 0;
            for (PipelinePart part : _instantiatedPipeline) {
                for (int i = 0; i < threadsPerTool; i++) {
                    System.out.printf("[Composer] Starting worker thread for pipeline part %s [%d/%d]\n", part.getUUID(), tId+1, _workers);
                    Thread thread = new Thread(new DUUISegmentedWorker(
                            tId,
                            _shutdownAtomic,
                            part,
                            collectionReader,
                            desc,
                            _storage,
                            name,
                            pipelineUUIDs
                    ));
                    thread.start();
                    threads.add(thread);
                    tId += 1;
                }
            }

            Instant starttime = Instant.now();
            while(!collectionReader.finishedLoading() || collectionReader.getDone() < collectionReader.getSize()) {
                System.out.println(collectionReader.getProgress());
                Thread.sleep(500L);
            }

            System.out.println("[Composer] All documents have been processed. Signaling threads to shut down now...");
            _shutdownAtomic.set(true);
            for(int i = 0; i < threads.size(); i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n", i+1, threads.size());
                threads.get(i).join();
                System.out.printf("[Composer] Thread %d returned.\n", i);
            }

            System.out.println("[Composer] Merging documents...");
            collectionReader.merge();

            if(_storage != null) {
                _storage.finalizeRun(name, starttime, Instant.now());
            }

            System.out.println("[Composer] All threads returned.");
            shutdown_pipeline();

        } catch (Exception e) {
            e.printStackTrace();

            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            shutdown_pipeline();

            throw e;
        }
    }

    /**
     * Runs the DUUI pipeline.
     * @param collectionReader CAS collection reader
     * @param name Run name
     * @throws Exception
     */
    public void run(AsyncCollectionReader collectionReader, String name) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<JCas> loadedCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        _shutdownAtomic.set(false);

        Exception catched = null;

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);

        try {
            if (_storage != null) {
                _storage.addNewRun(name, this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            if (_cas_poolsize == null) {
                _cas_poolsize = (int) Math.ceil(_workers * 1.5);
                System.out.printf("[Composer] Calculated CAS poolsize of %d!\n", _cas_poolsize);
            } else {
                if (_cas_poolsize < _workers) {
                    System.err.println("[Composer] WARNING: Pool size is smaller than the available threads, this is likely a bottleneck.");
                }
            }

            for (int i = 0; i < _cas_poolsize; i++) {
                emptyCasDocuments.add(JCasFactory.createJCas(desc));
            }

            Thread[] arr = new Thread[_workers];
            for (int i = 0; i < _workers; i++) {
                System.out.printf("[Composer] Starting worker thread [%d/%d]\n", i + 1, _workers);
                arr[i] = new DUUIWorkerAsyncReader(_instantiatedPipeline, emptyCasDocuments.poll(), _shutdownAtomic, aliveThreads, _storage, name, collectionReader, this);
                arr[i].start();
            }
            Instant starttime = Instant.now();
            final int maxNumberOfFutures = 20;
            CompletableFuture<Integer>[] futures = new CompletableFuture[maxNumberOfFutures];
            boolean breakit = false;
            while (!_shutdownAtomic.get()) {
                if (collectionReader.getCachedSize() > collectionReader.getMaxMemory()) {
                    Thread.sleep(50);
                    continue;
                }
                for (int i = 0; i < maxNumberOfFutures; i++) {
                    futures[i] = collectionReader.getAsyncNextByteArray();
                }
                CompletableFuture.allOf(futures).join();
                for (int i = 0; i < maxNumberOfFutures; i++) {
                    if (futures[i].join() != 0) {
                        breakit = true;
                    }
                }
                if (breakit) break;
            }

            AtomicInteger waitCount = new AtomicInteger();
            waitCount.set(0);
            // Wartet, bis die Dokumente fertig verarbeitet wurden.
            while (emptyCasDocuments.size() != _cas_poolsize && !collectionReader.isEmpty()) {
                if (waitCount.incrementAndGet() % 500 == 0) {
                    System.out.println("[Composer] Waiting for threads to finish document processing...");
                }
                Thread.sleep(1000 * _workers); // to fast or in relation with threads?

            }
            System.out.println("[Composer] All documents have been processed. Signaling threads to shut down now...");
            _shutdownAtomic.set(true);

            for (int i = 0; i < arr.length; i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n", i + 1, arr.length);
                arr[i].join();
                System.out.printf("[Composer] Thread %d returned.\n", i);
            }
            if (_storage != null) {
                _storage.finalizeRun(name, starttime, Instant.now());
            }
            System.out.println("[Composer] All threads returned.");
            shutdown_pipeline();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            shutdown_pipeline();
            throw e;
        }
    }

    /**
     * Runs the DUUI pipeline.
     * @param collectionReader CAS collection reader
     * @param name Run name
     * @throws Exception
     */
    private void run_async(CollectionReader collectionReader, String name) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<JCas> loadedCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        _shutdownAtomic.set(false);

        Exception catched = null;

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);

        try {
            if (_storage != null) {
                _storage.addNewRun(name, this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            if (_cas_poolsize == null) {
                _cas_poolsize = (int) Math.ceil(_workers * 1.5);
                System.out.printf("[Composer] Calculated CAS poolsize of %d!\n", _cas_poolsize);
            } else {
                if (_cas_poolsize < _workers) {
                    System.err.println("[Composer] WARNING: Pool size is smaller than the available threads, this is likely a bottleneck.");
                }
            }

            for (int i = 0; i < _cas_poolsize; i++) {
                emptyCasDocuments.add(JCasFactory.createJCas(desc));
            }

            Thread[] arr = new Thread[_workers];
            for (int i = 0; i < _workers; i++) {
                System.out.printf("[Composer] Starting worker thread [%d/%d]\n", i + 1, _workers);
                //TODO: Use Inputs and Outputs to create paralel execution plan
                //Implement new ExecutionPlanGenerator & ExecutionPlan
                arr[i] = new DUUIWorker(_instantiatedPipeline, emptyCasDocuments, loadedCasDocuments, _shutdownAtomic, aliveThreads, _storage, name, null,
                    new DUUILinearExecutionPlanGenerator(_instantiatedPipeline), this);
                arr[i].start();
            }
            Instant starttime = Instant.now();
            while (collectionReader.hasNext()) {
                JCas jc = emptyCasDocuments.poll();
                while (jc == null) {
                    jc = emptyCasDocuments.poll();
                }
                collectionReader.getNext(jc.getCas());
                loadedCasDocuments.add(jc);
            }
            AtomicInteger waitCount = new AtomicInteger();
            waitCount.set(0);
            while (emptyCasDocuments.size() != _cas_poolsize) {
                if (waitCount.getAndIncrement() % 500 == 0) {
                    System.out.println("[Composer] Waiting for threads to finish document processing...");
                }
                Thread.sleep(1000);
            }
            System.out.println("[Composer] All documents have been processed. Signaling threads to shut down now...");
            _shutdownAtomic.set(true);

            for (int i = 0; i < arr.length; i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n", i + 1, arr.length);
                arr[i].join();
                System.out.printf("[Composer] Thread %d returned.\n", i);
            }
            if (_storage != null) {
                _storage.finalizeRun(name, starttime, Instant.now());
            }
            System.out.println("[Composer] All threads returned.");
            shutdown_pipeline();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            shutdown_pipeline();
            throw e;
        }
    }

    /**
     * Runs the pipeline.
     * @param reader CAS collection reader
     * @throws Exception
     */
    public void run(CollectionReaderDescription reader) throws Exception {
        run(reader, null);
    }

    public Vector<DUUIPipelineComponent> getPipeline() {
        return _pipeline;
    }

    /**
     * Runs the pipeline.
     * @param reader CAS collection reader
     * @param name Run name
     * @throws Exception
     */
    public void run(CollectionReaderDescription reader, String name) throws Exception {
        Exception catched = null;
        if (_storage != null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }
        addEvent(
            DUUIEvent.Sender.COMPOSER,
            "Instantiating the collection reader..."
        );

        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);

        addEvent(
            DUUIEvent.Sender.COMPOSER,
            "Instantiated the collection reader"
        );

        if (_workers == 1) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "Running in synchronous mode, 1 thread at most!");

            _cas_poolsize = 1;
        } else {
            run_async(collectionReader, name);
            return;
        }

        try {
            if (_storage != null) {
                _storage.addNewRun(name, this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            JCas jc = JCasFactory.createJCas(desc);
            Instant starttime = Instant.now();
            while (collectionReader.hasNext()) {
                long waitTimeStart = System.nanoTime();
                collectionReader.getNext(jc.getCas());
                long waitTimeEnd = System.nanoTime();
                try {
                    run_pipeline(name, jc, waitTimeEnd - waitTimeStart, _instantiatedPipeline);
                } catch (Exception e) {
                    e.printStackTrace();

                    // If we want to track errors we just continue with the next document
                    // TODO this should be configurable separately
                    if (_storage == null) {
                        throw e;
                    }
                    if (!_storage.shouldTrackErrorDocs()) {
                        throw e;
                    }

                    addEvent(
                        DUUIEvent.Sender.COMPOSER,
                        "Something went wrong, shutting down remaining components...");
                }
                jc.reset();
            }
            if (_storage != null) {
                _storage.finalizeRun(name, starttime, Instant.now());
            }
        } catch (Exception e) {
            e.printStackTrace();
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "Something went wrong, shutting down remaining components...");
            catched = e;
        }

        shutdown_pipeline();
        if (catched != null) {
            throw catched;
        }
    }

    /**
     * Instantiates the DUUI pipeline.
     * <p>
     * This setups, starts and checks every pipeline component and requests their UIMA typesystem to merge all types needed to process the full pipeline.
     * @return Merged typesystem based on all components
     * @throws Exception
     */
    public TypeSystemDescription instantiate_pipeline() throws Exception {
        if (isServiceStarted)
            return fromInstantiatedPipeline();

        Timer timer = new Timer();
        timer.start();

        _hasShutdown = false;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        if (_skipVerification) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "Running without verification, no process calls will be made during initialization!");
        }

        // Reset "instantiated pipeline" as the components will duplicate otherwise
        // See https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/issues/34
        // TODO should this only be done in "resetPipeline"?
        //_instantiatedPipeline.clear();

        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(_minimalTypesystem);
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        try {
            int index = 0;

            for (DUUIPipelineComponent comp : _pipeline) {
                if (shouldShutdown()) return null;

                IDUUIDriverInterface driver = _drivers.get(comp.getDriver());
                pipelineStatus.put(driver.getClass().getSimpleName(), DUUIStatus.INSTANTIATING);
                pipelineStatus.put(comp.getName(), DUUIStatus.INSTANTIATING);

                // When a pipeline is run as a service, only components that are not yet instantiated
                // should be instantiated here.

                if (isServiceStarted && _instantiatedPipeline.size() > index) {
                    addEvent(
                        DUUIEvent.Sender.COMPOSER,
                        String.format("Reusing component %s", comp.getName())
                    );

                    TypeSystemDescription desc = driver.get_typesystem(_instantiatedPipeline.get(index).getUUID());
                    if (desc != null) {
                        descriptions.add(desc);
                    }
                } else {
                    addEvent(
                        DUUIEvent.Sender.COMPOSER,
                        String.format("Instantiating component %s", comp.getName())
                    );

                    String uuid = driver.instantiate(comp, jc, _skipVerification, _shutdownAtomic);
                    if (uuid == null) {
                        shutdown();
                        return null;
                    }

                    DUUISegmentationStrategy segmentationStrategy = comp.getSegmentationStrategy();

                    TypeSystemDescription desc = driver.get_typesystem(uuid);
                    if (desc != null) {
                        descriptions.add(desc);
                    }
                    //TODO: get input output of every annotator
                    _instantiatedPipeline.add(new PipelinePart(driver, uuid, comp.getName(), segmentationStrategy));
                }

                index++;
                pipelineStatus.put(comp.getName(), DUUIStatus.IDLE);
            }

            for (IDUUIDriverInterface driver : _drivers.values()) {
                pipelineStatus.put(driver.getClass().getSimpleName(), DUUIStatus.IDLE);
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

        if (isServiceStarted && instantiatedTypeSystem != null) {
            addEvent(DUUIEvent.Sender.COMPOSER, "Reusing TypeSystemDescription");
        } else {
            isServiceStarted = isService;

            if (descriptions.size() > 1) {
                instantiatedTypeSystem = CasCreationUtils.mergeTypeSystems(descriptions);
            } else if (descriptions.size() == 1) {
                instantiatedTypeSystem = descriptions.get(0);
            } else {
                instantiatedTypeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription();
            }
        }

        timer.stop();
        addEvent(
            DUUIEvent.Sender.COMPOSER,
            String.format("Instatiated Pipeline after %d ms.", timer.getDuration()));

        instantiationDuration = timer.getDuration();

        return instantiatedTypeSystem;
    }

    /**
     * Runs the pipeline for a single CAS object.
     * @param name Run name
     * @param jc CAS to process
     * @param documentWaitTime Time waited for document, for metrics
     * @param pipeline Component pipeline
     * @return Processed CAS object
     * @throws Exception
     */
    private JCas run_pipeline(String name, JCas jc, long documentWaitTime, Vector<PipelinePart> pipeline) throws Exception {
        progress.set(0);

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
        if (_storage != null) {
            trackErrorDocs = _storage.shouldTrackErrorDocs();
        }

        DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name, documentWaitTime, jc, trackErrorDocs);
        document.setStartedAt();
        document.setStatus(DUUIStatus.ACTIVE);

        Exception error = null;
        try {
            for (PipelinePart comp : pipeline) {
                if (shouldShutdown()) break;
                pipelineStatus.put(comp.getName(), DUUIStatus.ACTIVE);

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
                    comp.getDriver().run(comp.getUUID(), jc, perf, this);
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
            if (_storage == null) {
                throw exception;
            }
            if (!_storage.shouldTrackErrorDocs()) {
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

        if (_storage != null) {
            _storage.addMetricsForDocument(perf);
        }

        incrementProgress();
        return jc;
    }

    /**
     * Shuts down the pipeline to stop all components.
     * @throws Exception
     */
    private void shutdown_pipeline() throws Exception {
        if (!_instantiatedPipeline.isEmpty()) {
            for (PipelinePart comp : _instantiatedPipeline) {
                pipelineStatus.put(comp.getName(), DUUIStatus.SHUTDOWN);
                addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    String.format("Shutting down %s (%s)", comp.getName(), comp.getUUID()));

                boolean fullyShutdown = false;
                while (!fullyShutdown) {
                    fullyShutdown = comp.getDriver().destroy(comp.getUUID());
                }
                pipelineStatus.put(
                    comp.getName(),
                    DUUIStatus.INACTIVE);
            }
            _instantiatedPipeline.clear();
        }

        if (_monitor != null) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                String.format("Visit %s to view the data.", _monitor.generateURL()));
        }


    }

    /**
     * Prints the concurrency graph to std output.
     * @throws Exception
     */
    public void printConcurrencyGraph() throws Exception {
        Exception catched = null;
        try {
            instantiate_pipeline();
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                String.format(
                    "CAS Pool size %d",
                    Objects.requireNonNullElseGet(_cas_poolsize, () -> _workers)));

            addEvent(
                DUUIEvent.Sender.COMPOSER,
                String.format("Worker threads %d", _workers));

            for (PipelinePart comp : _instantiatedPipeline) {
                comp.getDriver().printConcurrencyGraph(comp.getUUID());
            }
        } catch (Exception e) {
            e.printStackTrace();

            addEvent(
                DUUIEvent.Sender.COMPOSER,
                String.format("%s Something went wrong, shutting down remaining components...", _instantiatedPipeline));

            catched = e;
        }
        shutdown_pipeline();
        if (catched != null) {
            throw catched;
        }
    }

    /**
     * Run the pipeline for a single CAS object.
     * @param jc CAS object to process
     * @throws Exception
     */
    public void run(JCas jc) throws Exception {
        run(jc, null);
    }

    /**
     * Run the pipeline for a single CAS object.
     * @param jc CAS object to process
     * @param name Run name
     * @throws Exception
     */
    public void run(JCas jc, String name) throws Exception {
        if (_storage != null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }
        Exception catched = null;
        if (_workers != 1) {
            System.err.println("[Composer] WARNING: Single document processing runs always single threaded, worker threads are ignored!");
        }

        try {
            if (_storage != null) {
                _storage.addNewRun(name, this);
            }
            Instant starttime = Instant.now();

            // dont instantiate pipeline for every run
            // See https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/issues/34
            // TODO check for side effects
            if (_instantiatedPipeline == null || _instantiatedPipeline.isEmpty()) {
                TypeSystemDescription desc = instantiate_pipeline();

                if (desc == null || shouldShutdown()) {
                    shutdown();
                    return;
                }
            }

            JCas start = run_pipeline(name, jc, 0, _instantiatedPipeline);

            if (_storage != null) {
                _storage.finalizeRun(name, starttime, Instant.now());
            }
        } catch (Exception e) {
            e.printStackTrace();
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "Something went wrong, shutting down remaining components...");
            catched = e;
        }
        /** shutdown **/
        //shutdown_pipeline();
        if (catched != null) {
            throw catched;
        }
    }

    public int getWorkerCount() {
        return _workers;
    }

    /**
     * Shuts down the DUUI controller by signaling every worker and stopping all components.
     * @throws UnknownHostException
     */
    public void shutdown() throws IOException, InterruptedException {
        if (isService) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "Process finished. Keeping pipeline active for further requests.");
            return;
        }

        if (_hasShutdown) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "Shutdown already happened. Skipping.");
            return;
        }

        addEvent(
            DUUIEvent.Sender.COMPOSER,
            "Starting shutdown.");

        _shutdownAtomic.set(true);

        if (_monitor != null) {
            addEvent(DUUIEvent.Sender.COMPOSER, "Shutting down monitor.");
            _monitor.shutdown();
            /**
             * @see
             * @Givara
             * @edited Dawit Terefe
             * Added option to keep connection open.
             */
//                if (!_connection_open) {
//                    _clients.forEach(IDUUIConnectionHandler::close);
//                }
        }

        if (_storage != null) {
            addEvent(DUUIEvent.Sender.COMPOSER, "Shutting down storage.");
            _storage.shutdown();
        }


        if (!_connection_open) {
            _clients.forEach(IDUUIConnectionHandler::close);
        }

        try {
            addEvent(DUUIEvent.Sender.COMPOSER, "Shutting down pipeline.");
            shutdown_pipeline();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (IDUUIDriverInterface driver : _drivers.values()) {
            addEvent(DUUIEvent.Sender.COMPOSER, "Shutting down driver " + driver.getClass().getSimpleName());
            pipelineStatus.put(driver.getClass().getSimpleName(), DUUIStatus.SHUTDOWN);
            driver.shutdown();
            pipelineStatus.put(driver.getClass().getSimpleName(), DUUIStatus.INACTIVE);
        }

        _hasShutdown = true;
        addEvent(DUUIEvent.Sender.COMPOSER, "Shutdown complete");
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

    /**
     * Run the pipeline using a {@link DUUIDocumentReader} to retrieve the data from a given source.
     *
     * @param documentReader The document reader containing the {@link org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIDocumentHandler} instances.
     * @param identifier     A unique identifier function as the key in the storage backend
     * @throws Exception
     */
    public void run(DUUIDocumentReader documentReader, String identifier) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        _shutdownAtomic.set(false);

        addEvent(
            DUUIEvent.Sender.COMPOSER,
            String.format("Running in asynchronous mode using up to %d threads", _workers));

        try {
            if (_storage != null) {
                _storage.addNewRun(identifier, this);
            }

            TypeSystemDescription desc = instantiate_pipeline();

            if (desc == null || shouldShutdown()) {
                shutdown();
                return;
            }

            if (_cas_poolsize == null) {
                _cas_poolsize = (int) Math.ceil(_workers * 1.5);
                addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    String.format("Calculated CAS poolsize of %d!", _cas_poolsize));

            } else {
                if (_cas_poolsize < _workers) {
                    addEvent(
                        DUUIEvent.Sender.COMPOSER,
                        "Pool size is smaller than the available threads, this is likely a bottleneck.",
                        DebugLevel.WARN);
                }
            }

            for (int i = 0; i < _cas_poolsize; i++) {
                if (shouldShutdown()) {
                    shutdown();
                    return;
                }

                addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    "Creating CAS " + (i + 1) + " / " + _cas_poolsize);

                emptyCasDocuments.add(JCasFactory.createJCas(desc));
            }

            Thread[] arr = new Thread[_workers];
            for (int i = 0; i < _workers; i++) {
                if (shouldShutdown()) {
                    shutdown();
                    return;
                }

                addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    String.format("Starting Thread %d / %d", i + 1, _workers));

                arr[i] = new DUUIWorkerDocumentReader(
                    _instantiatedPipeline,
                    emptyCasDocuments.poll(),
                    _shutdownAtomic,
                    aliveThreads,
                    _storage,
                    identifier,
                    documentReader,
                    this
                );

                arr[i].start();
            }

            Instant starttime = Instant.now();

            final int maxNumberOfFutures = 20;
            CompletableFuture<Integer>[] futures = new CompletableFuture[maxNumberOfFutures];
            boolean breakit = false;
            while (!_shutdownAtomic.get()) {
                if (documentReader.getCurrentMemorySize() > documentReader.getMaximumMemory()) {
                    Thread.sleep(50);
                    continue;
                }

                for (int i = 0; i < maxNumberOfFutures; i++) {
                    futures[i] = documentReader.getAsyncNextByteArray();
                }

                CompletableFuture.allOf(futures).join();
                for (int i = 0; i < maxNumberOfFutures; i++) {
                    if (futures[i].join() != 0) {
                        breakit = true;
                    }
                }
                if (breakit)
                    break;
            }

            while (emptyCasDocuments.size() != _cas_poolsize && documentReader.hasNext()) {
                try {
                    if (shouldShutdown())
                        break;
                    addEvent(DUUIEvent.Sender.COMPOSER, "Waiting for threads to finish");
                    Thread.sleep(1000L * _workers); // to fast or in relation with threads?
                } catch (InterruptedException e) {
                    break;
                }
            }

            if (shouldShutdown()) {
                addEvent(
                    DUUIEvent.Sender.COMPOSER,
                    String.format("Interrupted processing after %d documents", getProgress()));
            } else {
                _shutdownAtomic.set(true);
            }

            for (Thread thread : arr) {
                thread.join();
            }

            if (_storage != null) {
                _storage.finalizeRun(identifier, starttime, Instant.now());
            }

            addEvent(DUUIEvent.Sender.COMPOSER, "Process finished");
            isFinished.set(true);
            shutdown();
        } catch (InterruptedException ignored) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                "The process has been interrupted before finishing."
            );
        } catch (Exception e) {
            addEvent(
                DUUIEvent.Sender.COMPOSER,
                String.format("An exception occurred: %s", e.getMessage()), DebugLevel.ERROR);

            shutdown();
            throw e;
        }
    }

    /**
     * Allow access to the instantiated pipeline to store it for future reusability.
     *
     * @return an instantiated pipeline.
     */
    public Vector<PipelinePart> getInstantiatedPipeline() {
        return _instantiatedPipeline;
    }

    /**
     * Directly sets the instantiated pipeline.
     * @param pipeline Instantiated pipeline
     * @return this, for method chaining
     */
    public DUUIComposer withInstantiatedPipeline(Vector<PipelinePart> pipeline) {
        this._instantiatedPipeline = pipeline;
        this.isServiceStarted = true;
        return this;
    }

    /**
     * Generates merged typesystem of all components from already instantiated pipeline.
     * @return Merged typesystem
     * @throws ResourceInitializationException
     * @throws CompressorException
     * @throws IOException
     * @throws InterruptedException
     * @throws SAXException
     */
    public TypeSystemDescription fromInstantiatedPipeline() throws ResourceInitializationException, CompressorException, IOException, InterruptedException, SAXException {
        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(_minimalTypesystem);
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());

        for (PipelinePart part : _instantiatedPipeline) {
            addDriver(part.getDriver());
            TypeSystemDescription desc = part.getDriver().get_typesystem(part.getUUID());
            if (desc != null) {
                descriptions.add(desc);
            }
        }

        for (IDUUIDriverInterface driver : _drivers.values()) {
            pipelineStatus.put(driver.getClass().getSimpleName(), DUUIStatus.IDLE);
        }


        if (descriptions.size() > 1) {
            instantiatedTypeSystem = CasCreationUtils.mergeTypeSystems(descriptions);
        } else if (descriptions.size() == 1) {
            instantiatedTypeSystem = descriptions.get(0);
        } else {
            instantiatedTypeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription();
        }

        return instantiatedTypeSystem;
    }

    public List<DUUIEvent> getEvents() {
        return events;
    }

    /**
     * Add a new Event to the events list of the Composer. A {@link DUUIEvent} marks a significant timestamp
     * during a process.
     *
     * @param sender  The class or object adding the event.
     * @param message The message of the event.
     * @param debugLevel Debug level.
     */
    public void addEvent(DUUIEvent.Sender sender, String message, DebugLevel debugLevel) {
        DUUIEvent event = new DUUIEvent(sender, message, debugLevel);
        events.add(event);
        if (event.getDebugLevel().compareTo(this.debugLevel) <= 0
            && !this.debugLevel.equals(DebugLevel.NONE)) {
            System.out.println(event);
        }
    }

    /**
     * Adds an event to the composer.
     * @param sender DUUI module emitting the event
     * @param message Event message
     */
    public void addEvent(DUUIEvent.Sender sender, String message) {
        addEvent(sender, message, DebugLevel.DEBUG);
    }

    public Set<DUUIDocument> getDocuments() {
        return new HashSet<>(documents.values());
    }

    /**
     * Finds a {@link DUUIDocument} based on its path.
     * @param path Document path
     * @return Document
     */
    public DUUIDocument findDocumentByPath(String path) {
        return documents.get(path);
    }


    /**
     * Add a new {@link DUUIDocument} for processing
     *
     * @param document The document to add.
     * @return The added document if it is not already present in the set otherwise the existing document.
     */
    public DUUIDocument addDocument(DUUIDocument document) {
        if (documents.containsKey(document.getPath())) {
            return documents.get(document.getPath());
        }

        documents.put(document.getPath(), document);
        addEvent(
            DUUIEvent.Sender.COMPOSER,
            String.format("Added Document %s for processing", document.getPath()));

        return document;
    }

    /**
     * Adds multiple {@link DUUIDocument} for processing.
     * @param documents List of documents
     */
    public void addDocuments(Collection<DUUIDocument> documents) {
        for (DUUIDocument document : documents) {
            DUUIDocument ignored = addDocument(document);
        }
    }

    /**
     * Collects all {@link DUUIDocument} paths
     * @return Set of document paths
     */
    public Set<String> getDocumentPaths() {
        return documents
            .values()
            .stream()
            .map(DUUIDocument::getPath)
            .collect(Collectors.toSet());
    }

    public Map<String, String> getPipelineStatus() {
        return pipelineStatus;
    }

    /**
     * Tracks the current status of Drivers and Components.
     *
     * @param name   The identifier of the object.
     * @param status The status the object is in.
     */
    public void setPipelineStatus(String name, String status) {
        pipelineStatus.put(name, status);
    }

    public DebugLevel getDebugLevel() {
        return debugLevel;
    }

    public boolean getIgnoreErrors() {
        return ignoreErrors;
    }

    /**
     * Enable error ignore.
     * @param ignoreErrors true to ignore errors, false by default
     * @return this, for method chaining
     */
    public DUUIComposer withIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return this;
    }

    public boolean isService() {
        return isService;
    }

    /**
     * When set to true the pipeline is not shutdown on completion but remains idle until a new request
     * is made.
     *
     * @param service Flag that prevents the shutdown of the pipeline .
     */
    public DUUIComposer asService(boolean service) {
        isService = service;
        return this;
    }

    /**
     * Allow the cancellation of a process by calling interrupt.
     *
     * @param reason Optional. Provide a reason for interrupting.
     */
    public void interrupt(String reason) {
        _shutdownAtomic.set(true);
        addEvent(DUUIEvent.Sender.COMPOSER, String.format("Execution has been interrupted. Reason: %s.", reason));
    }

    /**
     * Allow the cancellation of a process by calling interrupt.
     */
    public void interrupt() {
        interrupt("User request");
    }

    public void resetService() {
        events.clear();
        documents.clear();
        progress.set(0);
        _shutdownAtomic.set(false);
        isServiceStarted = isService;
    }

    public boolean isServiceStarted() {
        return isServiceStarted;
    }

    public boolean isFinished() {
        return isFinished.get();
    }

    public void setFinished(boolean isFinished) {
        this.isFinished.set(isFinished);
    }

    public long getInstantiationDuration() {
        return instantiationDuration;
    }

    /**
     * The instantiation duration is the time it takes to initialize all drivers and components. The duration
     * is measured as soon as the pipeline is operational.
     *
     * @param instantiationDuration Duration it takes to make the pipeline operational.
     */
    public void setInstantiationDuration(long instantiationDuration) {
        this.instantiationDuration = instantiationDuration;
    }

    public int getProgress() {
        return progress.get();
    }

    public AtomicInteger getProgressAtomic() { return progress;}

    public void incrementProgress() {
        int progress = this.progress.incrementAndGet();
        addEvent(
            DUUIEvent.Sender.COMPOSER,
            String.format("%d Documents have been processed", progress));
    }

    public boolean get_isServiceStarted() {
        return this.isServiceStarted;
    }

    public void set_isServiceStarted(boolean value) {
        this.isServiceStarted = value;
    }

    public void setServiceStarted(boolean serviceStarted) {
        isServiceStarted = serviceStarted;
    }

    public void set_hasShutdown(boolean _hasShutdown) {
        this._hasShutdown = _hasShutdown;
    }

    public boolean get_skipVerification() {
        return _skipVerification;
    }

    public TypeSystemDescription get_minimalTypesystem() {
        return _minimalTypesystem;
    }

    public Vector<DUUIPipelineComponent> get_pipeline() {
        return _pipeline;
    }

    public Map<String, IDUUIDriverInterface> get_drivers() {
        return _drivers;
    }

    public IDUUIStorageBackend get_storage() {
        return _storage;
    }

    public Vector<PipelinePart> get_instantiatedPipeline() {
        return _instantiatedPipeline;
    }

    public AtomicBoolean get_shutdownAtomic() {
        return _shutdownAtomic;
    }

    public TypeSystemDescription getInstantiatedTypeSystem() {
        return instantiatedTypeSystem;
    }

    public void setInstantiatedTypeSystem(TypeSystemDescription instantiatedTypeSystem) {
        this.instantiatedTypeSystem = instantiatedTypeSystem;
    }



    /**
     * If debug is enabled Events will be written to standard out
     *
     * @param debugLevel The level at which events are written to standard out.
     * @return This Composer
     */
    public DUUIComposer withDebugLevel(DebugLevel debugLevel) {
        this.debugLevel = debugLevel;
        return this;
    }

    public boolean shouldShutdown() {
        return _shutdownAtomic.get();
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

    public static void main(String[] args) throws Exception {
        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json", DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUIComposer composer = new DUUIComposer()
            //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
            .withLuaContext(ctx)
            .withSkipVerification(true)
            .withWorkers(2);

        // Instantiate drivers with options
//        DUUIDockerDriver driver = new DUUIDockerDriver()
//                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
            .withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver()
            .withSwarmVisualizer(18872);

        // A driver must be added before components can be added for it in the composer.
//        composer.addDriver(driver);
        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

//        composer.addDriver(swarm_driver);

        // Every component needs a driver which instantiates and runs them
        // Local driver manages local docker container and pulls docker container from remote repositories
        /*composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUILocalDriver.Component("kava-i.de:5000/secure/test_image")*/

        //composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
        //                .withScale(4),
        //        DUUIUIMADriver.class);
        /*  composer.add(new DUUILocalDriver.Component("java_segmentation:latest")*/

        //composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
        //        .withScale(4)
        //        .build()
        //);
        // composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/benchmark_serde_echo_msgpack:0.2")
        //         .build());
//        composer.add(new DUUILocalDriver.Component("java_segmentation:latest")
//                        .withScale(1)
//                , DUUILocalDriver.class);
//        composer.add(new DUUIDockerDriver.Component("gnfinder:0.1")
//                        .withScale(1)
//                , DUUIDockerDriver.class);
        // input: [], outputs: [Token, Sentences]
        //composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715"));
        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-uima-service-gervader:0.5"));

        //composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)));
//                , DUUIRemoteDriver.class);*/

        // Remote driver handles all pure URL endpoints
        // composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
        //                 .withScale(1));

        //  composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.Component("http://127.0.0.1:9715")
        //          .withParameter("fuchs","damn"));
       /* composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.Component("http://127.0.0.1:9714")
                        .withScale(1),
                org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.class);*/


        // Engine 1 -> track 1 schreibt
        // Engine 2 -> track 2 schreibt
        // Merge -> Vereint das in _InitialView


        // Engine 1 -> track 1 schreibt
        // Engine 2 -> track 2 schreibt
        // Merge -> Vereint das in _InitialView


        // SpaCy Lemma, POS, NER besser in NER, Precision
        // StanfordNlpNER NER

        // ClearNlpPosTagger
        // OpenNlpPosTagger

        // p

        // Input: [de.org.tudarmstadt.sentence, de.org.tudarmstadt.Token]
        // Output: []
        // composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/languagedetection:0.3").withScale(1));
/*
        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(1)
                , DUUISwarmDriver.class);

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/gnfinder:latest")
                .withScale(1)
                , DUUISwarmDriver.class);*/
        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715")
                .withScale(1).withWebsocket(true).build());
//        composer.add(new SocketIO("http://127.0.0.1:9715"));

        // ByteArrayInputStream stream;
        // stream.read

        String val = "Dies ist ein kleiner Test Text für Abies!";
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);

        String va2 = "Dies ist ein ganz kleiner Test Text für Abies!";
        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentLanguage("de");
        jc2.setDocumentText(val);

        // Run single document
        composer.run(jc, "fuchs");
        composer.run(jc2, "fuchs1");
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        XmlCasSerializer.serialize(jc.getCas(),out);
//        System.out.println(new String(out.toByteArray()));


        /*TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(jc.getTypeSystem());

        //CAS: Dependency, Sentence, Token
        //Bar Chart: Wie viele Dependecies, Wie viele Sentences ....
        // Named Entity Recognition: 1 Klasse fuer Orte => 10 Annotation vom Typ Ort
        // 1 Klasse fuer Personen => 100 Annotationen vom Typ Personen
        //for(Dependency meta : JCasUtil.select(jc, Dependency.class)) {
        //    meta.getDependent().
        //}

        /*String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_token_only.lua").toURI()));
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote");
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
        System.out.println(out.toString());*/

        OutputStream out2 = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(), out2);
        System.out.println(out2.toString());

        // Run Collection Reader

        /*composer.run(createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, "test_corpora/**.txt",
                TextReader.PARAM_LANGUAGE, "en"),"next11");*/
        /** @see **/
        composer.shutdown();

    }
}

