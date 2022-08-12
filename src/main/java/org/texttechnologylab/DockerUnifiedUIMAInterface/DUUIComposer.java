package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.dkpro.core.io.xmi.XmiReader;
import org.luaj.vm2.Globals;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

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

    DUUIWorker(Vector<DUUIComposer.PipelinePart> engineFlow, ConcurrentLinkedQueue<JCas> emptyInstance, ConcurrentLinkedQueue<JCas> loadedInstances, AtomicBoolean shutdown, AtomicInteger error,
               IDUUIStorageBackend backend, String runKey, AsyncCollectionReader reader, IDUUIExecutionPlanGenerator generator) {
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
    }

    @Override
    public void run() {
        int num = _threadsAlive.addAndGet(1);
        while(true) {
            JCas object = null;
            long waitTimeStart = System.nanoTime();
            long waitTimeEnd = 0;
            while(object == null) {
                object = _loadedInstances.poll();

                if(_shutdown.get() && object == null) {
                    _threadsAlive.getAndDecrement();
                    return;
                }

                if(object==null && _reader!=null) {
                    object = _instancesToBeLoaded.poll();
                    if(object==null)
                        continue;
                    try {
                        waitTimeEnd = System.nanoTime();
                        if(!_reader.getNextCAS(object)) {
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
                    }
                }
            }
            if(waitTimeEnd==0) waitTimeEnd = System.nanoTime();
            IDUUIExecutionPlan execPlan = _generator.generate(object);

            //System.out.printf("[Composer] Thread %d still alive and doing work\n",num);

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(_runKey,
                    waitTimeEnd-waitTimeStart,
                    object);
            // f32, 64d, e57
            // DAG, Directed Acyclic Graph
                boolean done = false;
                List<Future<IDUUIExecutionPlan>> pendingFutures = new LinkedList<>();
                pendingFutures.add(execPlan.awaitMerge());
                while(!pendingFutures.isEmpty()) {
                    List<Future<IDUUIExecutionPlan>> newFutures = new LinkedList<>();
                    pendingFutures.removeIf(pending -> {
                        if (pending.isDone()) {
                            IDUUIExecutionPlan mergedPlan = null;
                            try {
                                mergedPlan = pending.get();
                                DUUIComposer.PipelinePart i = mergedPlan.getPipelinePart();
                                if(i!=null) {
                                    i.getDriver().run(i.getUUID(), mergedPlan.getJCas(), perf);
                                }
                                for (IDUUIExecutionPlan plan : mergedPlan.getNextExecutionPlans()) {
                                    newFutures.add(plan.awaitMerge());
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            } catch (CompressorException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            } catch (CASException e) {
                                e.printStackTrace();
                            } catch (AnalysisEngineProcessException e) {
                                e.printStackTrace();
                            } catch (SAXException e) {
                                e.printStackTrace();
                            }
                            return true;
                        }
                        return false;
                    });
                    pendingFutures.addAll(newFutures);
                }

            object.reset();
            _instancesToBeLoaded.add(object);
            if(_backend!=null) {
                _backend.addMetricsForDocument(perf);
            }
        }
    }
}

class DUUIWorkerAsyncReader extends Thread {
    Vector<DUUIComposer.PipelinePart> _flow;
    AtomicInteger _threadsAlive;
    AtomicBoolean _shutdown;
    IDUUIStorageBackend _backend;
    JCas _jc;
    String _runKey;
    AsyncCollectionReader _reader;

    DUUIWorkerAsyncReader(Vector<DUUIComposer.PipelinePart> engineFlow, JCas jc, AtomicBoolean shutdown, AtomicInteger error,
                          IDUUIStorageBackend backend, String runKey, AsyncCollectionReader reader) {
        super();
        _flow = engineFlow;
        _jc = jc;
        _shutdown = shutdown;
        _threadsAlive = error;
        _backend = backend;
        _runKey = runKey;
        _reader = reader;
    }

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

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(_runKey,
                    waitTimeEnd - waitTimeStart,
                    _jc);
            for (DUUIComposer.PipelinePart i : _flow) {
                try {
                    i.getDriver().run(i.getUUID(), _jc, perf);
                } catch (Exception e) {
                    //Ignore errors at the moment
                    e.printStackTrace();
                    System.out.println("Thread continues work!");
                }
            }

            if (_backend != null) {
                _backend.addMetricsForDocument(perf);
            }
        }
    }
}



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
    public static final String V1_COMPONENT_ENDPOINT_TYPESYSTEM = "/v1/typesystem";
    public static final String V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER = "/v1/communication_layer";

    private TypeSystemDescription _minimalTypesystem;


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
        _minimalTypesystem = TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/types/reproducibleAnnotations.xml").toURI().toString());
        System.out.println("[Composer] Initialised LUA scripting layer with version "+ globals.get("_VERSION"));

        DUUIComposer that = this;

        _shutdownHook = new Thread(() -> {
            try {
                System.out.println("[Composer] ShutdownHook... ");
                that.shutdown();
                System.out.println("[Composer] ShutdownHook finished.");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        });

        Runtime.getRuntime().addShutdownHook(_shutdownHook);
    }

    public DUUIComposer withMonitor(DUUIMonitor monitor) throws UnknownHostException, InterruptedException {
        _monitor = monitor;
        _monitor.setup();
        return this;
    }

    public DUUIComposer withSkipVerification(boolean skipVerification) {
        _skipVerification = skipVerification;
        return this;
    }

    public DUUIComposer withStorageBackend(IDUUIStorageBackend storage) throws UnknownHostException, InterruptedException {
        _storage = storage;
        return this;
    }

    public DUUIComposer withLuaContext(DUUILuaContext context) {
        _context = context;
        return this;
    }

    public DUUIComposer withCasPoolsize(int poolsize) {
        _cas_poolsize = poolsize;
        return this;
    }


    public DUUIComposer withWorkers(int workers) {
        _workers = workers;
        return this;
    }

    public DUUIComposer addDriver(IDUUIDriverInterface driver) {
        driver.setLuaContext(_context);
        _drivers.put(driver.getClass().getCanonicalName(), driver);
        return this;
    }

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

    public DUUIComposer add(DUUIDockerDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    public DUUIComposer add(DUUIUIMADriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    public DUUIComposer add(DUUIRemoteDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

    public DUUIComposer add(DUUISwarmDriver.Component object) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(object.build());
    }

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

    public DUUIComposer add(DUUIPipelineDescription desc) throws InvalidXMLException, IOException, SAXException, CompressorException {
        for(DUUIPipelineAnnotationComponent ann : desc.getComponents()) {
            add(ann.getComponent());
        }
        return this;
    }

    public static class PipelinePart {
        private final IDUUIDriverInterface _driver;
        private final String _uuid;

        PipelinePart(IDUUIDriverInterface driver, String uuid) {
            _driver = driver;
            _uuid = uuid;
        }

        public IDUUIDriverInterface getDriver() {
            return _driver;
        }

        public String getUUID() {
            return _uuid;
        }
    }

    public DUUIComposer resetPipeline() {
        _pipeline.clear();
        return this;
    }

    public void run(AsyncCollectionReader collectionReader, String name) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<JCas> loadedCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        _shutdownAtomic.set(false);

        Exception catched = null;

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);

        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            if (_cas_poolsize == null) {
                _cas_poolsize = (int)Math.ceil(_workers*1.5);
                System.out.printf("[Composer] Calculated CAS poolsize of %d!\n", _cas_poolsize);
            } else {
                if (_cas_poolsize < _workers) {
                    System.err.println("[Composer] WARNING: Pool size is smaller than the available threads, this is likely a bottleneck.");
                }
            }

            for(int i = 0; i < _cas_poolsize; i++) {
                emptyCasDocuments.add(JCasFactory.createJCas(desc));
            }

            Thread []arr = new Thread[_workers];
            for(int i = 0; i < _workers; i++) {
                System.out.printf("[Composer] Starting worker thread [%d/%d]\n",i+1,_workers);
                arr[i] = new DUUIWorkerAsyncReader(_instantiatedPipeline,emptyCasDocuments.poll(),_shutdownAtomic,aliveThreads,_storage,name,collectionReader);
                arr[i].start();
            }
            Instant starttime = Instant.now();
            final int maxNumberOfFutures = 20;
            CompletableFuture<Integer> []futures = new CompletableFuture[maxNumberOfFutures];
            boolean breakit = false;
            while(!_shutdownAtomic.get()) {
                if(collectionReader.getCachedSize() > collectionReader.getMaxMemory()) {
                    Thread.sleep(50);
                    continue;
                }
                for(int i = 0; i < maxNumberOfFutures; i++) {
                    futures[i] = collectionReader.getAsyncNextByteArray();
                }
                CompletableFuture.allOf(futures).join();
                for(int i = 0; i < maxNumberOfFutures; i++) {
                    if(futures[i].join() != 0) {
                        breakit=true;
                    }
                }
                if(breakit) break;
            }

            while(emptyCasDocuments.size() != _cas_poolsize && !collectionReader.isEmpty()) {
                System.out.println("[Composer] Waiting for threads to finish document processing...");
                Thread.sleep(1000);
            }
            System.out.println("[Composer] All documents have been processed. Signaling threads to shut down now...");
            _shutdownAtomic.set(true);

            for(int i = 0; i < arr.length; i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n",i+1,arr.length);
                arr[i].join();
                System.out.printf("[Composer] Thread %d returned.\n",i);
            }
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
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

    private void run_async(CollectionReader collectionReader, String name) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<JCas> loadedCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        _shutdownAtomic.set(false);

        Exception catched = null;

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);

        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            if (_cas_poolsize == null) {
                _cas_poolsize = (int)Math.ceil(_workers*1.5);
                System.out.printf("[Composer] Calculated CAS poolsize of %d!\n", _cas_poolsize);
            } else {
                if (_cas_poolsize < _workers) {
                    System.err.println("[Composer] WARNING: Pool size is smaller than the available threads, this is likely a bottleneck.");
                }
            }

            for(int i = 0; i < _cas_poolsize; i++) {
                emptyCasDocuments.add(JCasFactory.createJCas(desc));
            }

            Thread[] arr = new Thread[_workers];
            for (int i = 0; i < _workers; i++) {
                System.out.printf("[Composer] Starting worker thread [%d/%d]\n", i + 1, _workers);
                // TODO ParallelExecutionPlan and -Generator
                arr[i] = new DUUIWorker(_instantiatedPipeline, emptyCasDocuments, loadedCasDocuments, _shutdownAtomic, aliveThreads, _storage, name, null,
                        new DUUILinearExecutionPlanGenerator(_instantiatedPipeline));
                arr[i].start();
            }
            Instant starttime = Instant.now();
            while(collectionReader.hasNext()) {
                JCas jc = emptyCasDocuments.poll();
                while(jc == null) {
                    jc = emptyCasDocuments.poll();
                }
                collectionReader.getNext(jc.getCas());
                loadedCasDocuments.add(jc);
            }

            while(emptyCasDocuments.size() != _cas_poolsize) {
                System.out.println("[Composer] Waiting for threads to finish document processing...");
                Thread.sleep(1000);
            }
            System.out.println("[Composer] All documents have been processed. Signaling threads to shut down now...");
            _shutdownAtomic.set(true);

            for(int i = 0; i < arr.length; i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n",i+1,arr.length);
                arr[i].join();
                System.out.printf("[Composer] Thread %d returned.\n",i);
            }
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
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

    public void run(CollectionReaderDescription reader) throws Exception {
        run(reader,null);
    }

    public Vector<DUUIPipelineComponent> getPipeline() {
        return _pipeline;
    }

    public void run(CollectionReaderDescription reader, String name) throws Exception {
        Exception catched = null;
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }
        System.out.println("[Composer] Instantiating the collection reader...");
        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);
        System.out.println("[Composer] Instantiated the collection reader.");


        if(_workers == 1) {
            System.out.println("[Composer] Running in synchronous mode, 1 thread at most!");
            _cas_poolsize = 1;
        }
        else {
            run_async(collectionReader,name);
            return;
        }

        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            TypeSystemDescription desc = instantiate_pipeline();
            JCas jc = JCasFactory.createJCas(desc);
            Instant starttime = Instant.now();
            while(collectionReader.hasNext()) {
                long waitTimeStart = System.nanoTime();
                collectionReader.getNext(jc.getCas());
                long waitTimeEnd = System.nanoTime();
                run_pipeline(name,jc,waitTimeEnd-waitTimeStart,_instantiatedPipeline);
                jc.reset();
            }
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }

        shutdown_pipeline();
        if (catched != null) {
            throw catched;
        }
    }

    private TypeSystemDescription instantiate_pipeline() throws Exception {
        _hasShutdown = false;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        if(_skipVerification) {
            System.out.println("[Composer] Running without verification, no process calls will be made during initialization!");
        }

        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(_minimalTypesystem);
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        try {
            for (DUUIPipelineComponent comp : _pipeline) {
                IDUUIDriverInterface driver = _drivers.get(comp.getDriver());
                String uuid = driver.instantiate(comp, jc, _skipVerification);

                TypeSystemDescription desc = driver.get_typesystem(uuid);
                if (desc != null) {
                    descriptions.add(desc);
                }
                _instantiatedPipeline.add(new PipelinePart(driver, uuid));
            }

            // UUID und die input outputs
            // Execution Graph
            // Gegeben Knoten n finde Vorgaenger
            // inputs: [], outputs: [Token]
            // input: [Sentences], outputs: [POS]
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            throw e;
        }
        if(descriptions.size() > 1) {
            return CasCreationUtils.mergeTypeSystems(descriptions);
        }
        else if(descriptions.size() == 1) {
            return descriptions.get(0);
        }
        else {
            return TypeSystemDescriptionFactory.createTypeSystemDescription();
        }
    }

    private JCas run_pipeline(String name, JCas jc, long documentWaitTime, Vector<PipelinePart> pipeline) throws Exception {
        DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name,documentWaitTime,jc);
        for (PipelinePart comp : pipeline) {
            comp.getDriver().run(comp.getUUID(), jc, perf);
        }

        if(_storage!=null) {
            _storage.addMetricsForDocument(perf);
        }

        return jc;
    }

    private void shutdown_pipeline() throws Exception {
        if(!_instantiatedPipeline.isEmpty()) {
            for (PipelinePart comp : _instantiatedPipeline) {
                System.out.printf("[Composer] Shutting down %s...\n", comp.getUUID());
                comp.getDriver().destroy(comp.getUUID());
            }
            _instantiatedPipeline.clear();
            System.out.println("[Composer] Shut down complete.");
        }

        if(_monitor!=null) {
            System.out.printf("[Composer] Visit %s to view the data.\n",_monitor.generateURL());
        }
    }

    public void printConcurrencyGraph() throws Exception {
        Exception catched = null;
        try {
            instantiate_pipeline();
            System.out.printf("[Composer]: CAS Pool size %d\n", Objects.requireNonNullElseGet(_cas_poolsize, () -> _workers));
            System.out.printf("[Composer]: Worker threads %d\n", _workers);
            for (PipelinePart comp : _instantiatedPipeline) {
                comp.getDriver().printConcurrencyGraph(comp.getUUID());
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            System.out.println(_instantiatedPipeline+"\t[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }
        shutdown_pipeline();
        if (catched != null) {
            throw catched;
        }
    }

    public void run(JCas jc) throws Exception {
        run(jc,null);
    }

    public void run(JCas jc, String name) throws Exception {
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }
        Exception catched = null;
        if(_workers!=1) {
            System.err.println("[Composer] WARNING: Single document processing runs always single threaded, worker threads are ignored!");
        }

        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            Instant starttime = Instant.now();

            instantiate_pipeline();
            JCas start = run_pipeline(name,jc,0,_instantiatedPipeline);

            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }
        shutdown_pipeline();
        if (catched != null) {
            throw catched;
        }
    }

    public int getWorkerCount() {
        return _workers;
    }


    public void shutdown() throws UnknownHostException {
        if(!_hasShutdown) {
            _shutdownAtomic.set(true);
            if (_monitor != null) {
                _monitor.shutdown();
            } else if (_storage != null) {
                _storage.shutdown();
            }
            try {
                shutdown_pipeline();
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (IDUUIDriverInterface driver : _drivers.values()) {
                driver.shutdown();
            }
            _hasShutdown = true;
        }
        else {
            System.out.println("Skipped shutdown since it already happened!");
        }
    }


    public static void main(String[] args) throws Exception {
        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUIComposer composer = new DUUIComposer()
        //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(2);

        // Instantiate drivers with options
        DUUIDockerDriver driver = new DUUIDockerDriver()
                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
            //    .withSwarmVisualizer();

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(driver);
        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);
        composer.addDriver(swarm_driver);

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
        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715"));

//                , DUUIRemoteDriver.class);*/

        // Remote driver handles all pure URL endpoints
       // composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
       //                 .withScale(1));
/*
        //composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.Component("http://127.0.0.1:9714")
        composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.Component("http://127.0.0.1:9714")
                        .withScale(1),
                org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.class);*/


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
        /*composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/languagedetection:0.3")
                .withScale(1)
                , DUUISwarmDriver.class);

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(1)
                , DUUISwarmDriver.class);

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/gnfinder:latest")
                .withScale(1)
                , DUUISwarmDriver.class);*/

        //composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.Component("http://127.0.0.1:9715")
        //                .withScale(1).build());

       // ByteArrayInputStream stream;
       // stream.read

        String val2 = "Dies ist ein kleiner Test Text für Abies!";
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val2);

        // Run single document
        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE,"de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA,false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA,false,
                XmiReader.PARAM_LENIENT,true,
                XmiReader.PARAM_SOURCE_LOCATION,"/home/alexander/Documents/Corpora/German-Political-Speeches-Corpus/processed/*.xmi"),"fuchs");


        /*String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_token_only.lua").toURI()));
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote");
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
        System.out.println(out.toString());*/

        OutputStream out2 = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(),out2);
        System.out.println(out2.toString());

        // Run Collection Reader

        /*composer.run(createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, "test_corpora/**.txt",
                TextReader.PARAM_LANGUAGE, "en"),"next11");*/
        composer.shutdown();
  }
}

