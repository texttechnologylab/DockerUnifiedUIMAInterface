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
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.dkpro.core.io.xmi.XmiReader;
import org.luaj.vm2.Globals;
import org.luaj.vm2.lib.jse.JsePlatform;

import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelExecutionPipeline;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.apache.uima.jcas.tcas.Annotation;

import org.xml.sax.SAXException;

import java.io.IOException;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

                } catch (Exception e) {
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
                    //e.printStackTrace();
                    System.err.println(e.getMessage());
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

    private ExecutorService _executorService;
    private int _workers;
    public Integer _cas_poolsize;
    private DUUILuaContext _context;
    private DUUIMonitor _monitor;
    private IDUUIStorageBackend _storage;
    private boolean _skipVerification;

    private Vector<PipelinePart> _instantiatedPipeline;
    private DUUIParallelExecutionPipeline _executionPipeline;
    private Thread _shutdownHook;
    private AtomicBoolean _shutdownAtomic;
    private boolean _hasShutdown;

    private static final String DRIVER_OPTION_NAME = "duuid.composer.driver";
    public static final String COMPONENT_COMPONENT_UNIQUE_KEY = "duuid.storage.componentkey";

    public static final String V1_COMPONENT_ENDPOINT_PROCESS = "/v1/process";
    public static final String V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET = "/v1/process_websocket";
    public static final String V1_COMPONENT_ENDPOINT_TYPESYSTEM = "/v1/typesystem";
    public static final String V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER = "/v1/communication_layer";
    public static final String V1_COMPONENT_ENDPOINT_INPUT_OUTPUTS = "/v1/details/input_output"; 

    public static List<IDUUIConnectionHandler> _clients = new ArrayList<>();
    private boolean _connection_open = false;

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
        _executorService = Executors.newSingleThreadExecutor();
        _minimalTypesystem = TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/types/reproducibleAnnotations.xml").toURI().toString());
        System.out.println("[Composer] Initialised LUA scripting layer with version "+ globals.get("_VERSION"));

        DUUIComposer that = this;

        _shutdownHook = new Thread(() -> {
                try {
                    System.out.println("[Composer] ShutdownHook... ");
                    /** @see */
                    that.shutdown();
                    System.out.println("[Composer] ShutdownHook finished.");
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            });

        Runtime.getRuntime().addShutdownHook(_shutdownHook);
        
    }

    public int getWorkerCount() {
        return _workers;
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

    public DUUIComposer withOpenConnection(boolean open) {
        _connection_open = open;
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

    public DUUIComposer add(IDUUIDriverComponent<?> object) throws InvalidXMLException, IOException, SAXException, CompressorException {
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
                Thread.sleep(1000*_workers); // to fast or in relation with threads?
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

    public List<JCas> run(CollectionReaderDescription reader) throws Exception {
        return run(reader,null);
    }

    public List<JCas> run(CollectionReaderDescription reader, String name) throws Exception {

        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }
        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);
        System.out.println("[Composer] Instantiated the collection reader.");

        return run(collectionReader, name);
    }

    public List<JCas> run(CollectionReader collectionReader, String name) throws Exception {
        
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }

        TypeSystemDescription desc = instantiate_pipeline();

        System.out.println("[Composer] Generating pipeline-dependency graph.");
        _executionPipeline.printPipeline(name != null ? name : "");

        Callable<List<JCas>> runPipeline = () -> run_pipeline(name, collectionReader, desc);

        return run(name, runPipeline);

    }

    public void run(JCas jc) throws Exception {
        run(jc,null);
    }

    public void run(JCas jc, String name) throws Exception {
        
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }

        instantiate_pipeline();

        System.out.println("[Composer] Generating pipeline-dependency graph.");
        _executionPipeline.printPipeline(name != null ? name : "");
        
        Callable<JCas> runPipeline;
        if (_workers > 1) {
            runPipeline = () -> run_pipeline(name, jc, _executionPipeline);
        } else {
            runPipeline = () -> run_pipeline(name, jc, _instantiatedPipeline);
        }

        run(name, runPipeline);
    }

    private <T> T run(String name, Callable<T> runPipeline) throws Exception {

        Exception catched = null;
        T result = null; 

        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            Instant starttime = Instant.now();

            result = runPipeline.call();

            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }
        /** shutdown **/
        shutdown_pipeline();
        if (catched != null) {
            shutdown();
            throw catched;
        }

        return result;
    }

    public void shutdown() throws UnknownHostException {
        if(!_hasShutdown) {
            _shutdownAtomic.set(true);
            if (_monitor != null) {
                _monitor.shutdown();
            } else if (_storage != null) {
                _storage.shutdown();
            }
            if (!_connection_open) {
                _clients.forEach(IDUUIConnectionHandler::close);
            }
            try {
                shutdown_pipeline();
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (IDUUIDriverInterface driver : _drivers.values()) {
                driver.shutdown();
            }
            
            _executorService.shutdownNow(); 
            if (_executorService.isTerminated()) System.out.println("[DUUIComposer] Executor terminated!");
            if (!_executorService.isTerminated()) System.out.println("[DUUIComposer] Executor did not terminate!");
            _hasShutdown = true;
        }
        else {
            System.out.println("Skipped shutdown since it already happened!");
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

    private TypeSystemDescription instantiate_pipeline() throws Exception {
        _hasShutdown = false;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        if(_skipVerification) {
            System.out.println("[Composer] Running without verification, no process calls will be made during initialization!");
        }
        if (_workers > 1)
            _executorService = Executors.newFixedThreadPool(_workers);
        else _executorService = Executors.newSingleThreadExecutor();
        List<Callable<Integer>> tasks = new ArrayList<>();
        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(_minimalTypesystem);
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());

        try { 
            _pipeline.forEach(comp ->
                tasks.add(
                () -> {
                    IDUUIDriverInterface driver = _drivers.get(comp.getDriver());
                    String uuid;
                        uuid = driver.instantiate(comp, jc, _skipVerification);
                        
                        TypeSystemDescription desc = driver.get_typesystem(uuid);

                        if (desc != null)
                            synchronized (descriptions) {descriptions.add(desc);}
                        
                        AnnotatorSignature signature = driver.get_signature(uuid);
                        
                        synchronized (_instantiatedPipeline) {
                            _instantiatedPipeline.add(new PipelinePart(driver, uuid, signature));
                        }
                    return 1;  
                }   
            ));

            for (Future<?> f: _executorService.invokeAll(tasks))
                f.get();

            if (_workers > 1)
                _executionPipeline = new DUUIParallelExecutionPipeline(_instantiatedPipeline);
            else _executorService.shutdown(); 
            
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

    private JCas run_pipeline(String name, JCas jc, Vector<PipelinePart> pipeline) throws Exception {
        DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name, 0, jc);
        for (PipelinePart comp : pipeline) {
            comp.run(name, jc, perf);;
        }

        if(_storage!=null) {
            _storage.addMetricsForDocument(perf);
        }

        return jc;
    }

    private JCas run_pipeline(String name, JCas jc, DUUIParallelExecutionPipeline pipeline) 
    throws Exception {

        DUUIPipelineDocumentPerformance perf = 
            new DUUIPipelineDocumentPerformance(name, 0, jc);
        run_pipeline(jc, name, perf);
        _executorService.shutdown();
        _executorService.shutdownNow();
        _executorService.awaitTermination(100, TimeUnit.NANOSECONDS);
        if(_storage!=null) {
            _storage.addMetricsForDocument(perf);
        }

        return jc;
    }

    private List<JCas> run_pipeline(String name, CollectionReader reader, TypeSystemDescription desc) 
        throws Exception {

        List<Future<Boolean>> workers = new ArrayList<>();
        Map<Integer, JCas> results = new ConcurrentHashMap<>();

        int[] c = {1};
        while(reader.hasNext()) {
            // TODO: OutOfMemoryException 
            JCas jc = JCasFactory.createJCas(desc);
            long waitTimeStart = System.nanoTime();
            synchronized (reader) { reader.getNext(jc.getCas()); }
            long waitTimeEnd = System.nanoTime();

            workers.add(
                _executorService.submit(() -> {
                    DUUIPipelineDocumentPerformance perf = 
                        new DUUIPipelineDocumentPerformance(name + c[0], 
                                                            waitTimeEnd-waitTimeStart,
                                                            jc);
                    run_pipeline(jc, name, perf);

                    if(_storage!=null) {
                        _storage.addMetricsForDocument(perf);
                    }
                    if (c[0] == 1) results.put(c[0], jc);
                    return true; 
                })
            );
            c[0] += 1;
        }

        for (Future<?> f: workers)
            f.get();

        _executorService.shutdown();
        while (!_executorService.awaitTermination(1, TimeUnit.SECONDS)) {}

        return results.values().stream().collect(Collectors.toList());
    }

    private void run_pipeline(JCas jc, String name, DUUIPipelineDocumentPerformance perf) throws Exception {

        List<Future<Void>> workers = _executorService.invokeAll(
            _executionPipeline.getWorkers(name, jc, perf));

        for (Future<?> f: workers)
                f.get();
    }

    private void shutdown_pipeline() throws Exception {
        if(!_instantiatedPipeline.isEmpty()) {
            _instantiatedPipeline.forEach(PipelinePart::shutdown);
            _instantiatedPipeline.clear();
            System.out.println("[Composer] Shut down complete.");
        }

        if(_monitor!=null) {
            System.out.printf("[Composer] Visit %s to view the data.\n",_monitor.generateURL());
        }
    }

    public Vector<DUUIPipelineComponent> getPipeline() {
        return _pipeline;
    }

    public DUUIComposer resetPipeline() {
        _pipeline.clear();
        return this;
    }

    public class PipelinePart {
        private final IDUUIDriverInterface _driver;
        private final String _uuid;
        private final AnnotatorSignature _signature; 

        PipelinePart(IDUUIDriverInterface driver, String uuid) {
            _driver = driver;
            _uuid = uuid;
            _signature = null;
        }

        public PipelinePart(IDUUIDriverInterface driver, String uuid, AnnotatorSignature signature) {
            _driver = driver;
            _uuid = uuid;
            _signature = signature;
        }

        public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws AnalysisEngineProcessException, CASException, InterruptedException, IOException, SAXException, CompressorException {
            _driver.run(_uuid, jc, perf);
        }

        public void shutdown() {
            System.out.printf("[Composer] Shutting down %s...\n", _uuid);
            _driver.destroy(_uuid);
        }

        public AnnotatorSignature getSignature() {
            return _signature; 
        }

        public IDUUIDriverInterface getDriver() {
            return _driver;
        }

        public String getUUID() {
            return _uuid;
        }
    }

    public static class AnnotatorSignature {

        private final List<Class<? extends Annotation>> inputs;
        private final List<Class<? extends Annotation>> outputs;
    
        public AnnotatorSignature(List<Class<? extends Annotation>> inputs, List<Class<? extends Annotation>> outputs) {
            this.inputs = inputs;
            this.outputs = outputs;
        }
    
        public List<Class<? extends Annotation>> getInputs() {
            return inputs;
        }
    
        public List<Class<? extends Annotation>> getOutputs() {
            return outputs;
        }

        @Override
        public boolean equals(Object s2) {
            
            if (!(s2 instanceof AnnotatorSignature)) return false;

            return ((AnnotatorSignature)s2).getInputs().equals(this.getInputs()) &&
            ((AnnotatorSignature)s2).getOutputs().equals(this.getOutputs());
        }

        @Override
        public String toString() {
            StringBuilder signature = new StringBuilder();

            signature.append("[");
            this.inputs.forEach( input ->
                signature.append(input.getSimpleName() + " ")
            );
            signature.append("]");
            signature.append(" => ");
            signature.append("[");
            this.outputs.forEach( output ->
            signature.append(output.getSimpleName() + " ")
            );
            signature.append("]");

            return signature.toString().replaceAll(" ]", "]");
        }

        public int compareSignatures(AnnotatorSignature s2) {

            boolean s1DependentOnS2 = this.getInputs().stream()
            .anyMatch(s2.getOutputs()::contains);
            
            boolean s2DependentOnS1 = s2.getInputs().stream()
                .anyMatch(this.getOutputs()::contains);

            if (s1DependentOnS2 && s2DependentOnS1) {
                return -2; // Error: Cycle 
            } else if (s2DependentOnS1) {
                return 1; // Full-Dependency edge from s1 to s2
            } else if (s1DependentOnS2) {
                return -1; // Full-Dependency edge from s2 to s1
            } else {
                return 0; // No-Edge
            } 
        }
    }

    public static void main(String[] args) throws Exception {
        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        // DUUIComposer composer = new DUUIComposer()
        // //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
        //         .withLuaContext(ctx)
        //         .withSkipVerification(true)
        //         .withWorkers(2);

        // Instantiate drivers with options
//        DUUIDockerDriver driver = new DUUIDockerDriver()
//                .withTimeout(10000);

        // DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        // DUUIUIMADriver uima_driver = new DUUIUIMADriver()
        //         .withDebug(true);
        // DUUISwarmDriver swarm_driver = new DUUISwarmDriver()
        //         .withSwarmVisualizer(18872);

        // A driver must be added before components can be added for it in the composer.
//        composer.addDriver(driver);
        // composer.addDriver(remote_driver);
        // composer.addDriver(uima_driver);

//        composer.addDriver(swarm_driver);

        // Every component needs a driver which instantiates and runs them
        // Local driver manages local docker container and pulls docker container from remote repositories
        /*composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUILocalDriver.Component("kava-i.de:5000/secure/test_image")*/
   
       // composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/languagedetection:0.3").withScale(1));
/*
        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(1)
                , DUUISwarmDriver.class);

        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/gnfinder:latest")
                .withScale(1)
                , DUUISwarmDriver.class);*/
        // composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9715")
        //                 .withScale(1).withWebsocket(true).build());

       // ByteArrayInputStream stream;
       // stream.read

        DUUIComposer composer = new DUUIComposer()
            .withWorkers(10)
            .withSkipVerification(true)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIDockerDriver(), new DUUIRemoteDriver());
        composer.add( // 
        new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-ner:latest")
            .withImageFetching());
        composer.add( // 
        new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-lemmatizer:latest")
            .withImageFetching());
        composer.add( // 
        new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-morphologizer:latest")
            .withImageFetching());
        composer.add( // 
        new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-parser:latest")
            .withImageFetching());
        composer.add( // 
        new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-sentencizer:latest")
            .withImageFetching());
        composer.add( // 
        new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
            .withImageFetching());
        // composer.add( // 
        // new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
        // );
        // composer.add(new DUUIDockerDriver.Component("").withScale(0).withGPU(false));
        // composer.add( // 
        // new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
        // );
        // composer.add( // 
        // new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
        // );
        // composer.add( // 
        // new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
        // );
        // composer.add( // 
        // new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
        // );

        String val = "Dies ist ein kleiner Test Text für Abies!";
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val);

        JCas jc2 = JCasFactory.createJCas();
        jc2.setDocumentLanguage("de");
        jc2.setDocumentText(val);

        // Run single document
        composer.run(jc,"fuchs");
        
        composer.withWorkers(1);
        composer.run(jc2,"fuchs");
        composer.shutdown();

        System.out.println(
            Objects.equals(JCasUtil.select(jc, TOP.class).stream().count(), JCasUtil.select(jc2, TOP.class).stream().count())
        );
        // OutputStream out = new ByteArrayOutputStream();

        // OutputStream out2 = new ByteArrayOutputStream();
        // XmiCasSerializer.serialize(jc.getCas(),out);
        // XmiCasSerializer.serialize(jc2.getCas(),out2);
        // // System.out.println(out2.toString());
        
        // String wsText = out.toString()
        //     .replaceAll("<duui:ReproducibleAnnotation.*/>", "")
        //     .replaceAll("timestamp=\"[0-9]*\"", "0");
        // String restText = out2.toString()
        //     .replaceAll("<duui:ReproducibleAnnotation.*/>", "")
        //     .replaceAll("timestamp=\"[0-9]*\"", "0");
        
        // assertEquals(restText, wsText);
        
        
        // CollectionReaderDescription reader = null;
        // reader = org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription(XmiReader.class,
        //         XmiReader.PARAM_SOURCE_LOCATION,  "C:\\Users\\davet\\projects\\DockerUnifiedUIMAInterface\\src\\main\\resources\\sample\\**.gz.xmi.gz",
        //         XmiReader.PARAM_SORT_BY_SIZE, true
        // );

        // JCas jc = composer.run(reader, "Fuchs").get(0);
  }

} 



