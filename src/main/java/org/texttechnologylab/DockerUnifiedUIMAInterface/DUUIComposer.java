package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.BinaryCasSerDes4;
import org.apache.uima.cas.impl.BinaryCasSerDes4.CasCompare;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.CasUtil;
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
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.xml.sax.SAXException;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

import java.io.IOException;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler.measureStart;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler.measureEnd;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler.printMeasurements;

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
        Thread main = Thread.currentThread(); 
        _shutdownHook = new Thread(() -> {
                try {
                    
                    System.out.println("[Composer] ShutdownHook... ");
                    /** @see */
                    that.shutdown();
                    System.out.println("[Composer] ShutdownHook finished.");
                } catch (UnknownHostException | InterruptedException e) {
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

    public DUUIComposer add(IDUUIDriverComponent<?> component) throws InvalidXMLException, IOException, SAXException, CompressorException {
        return add(component.build());
    }

    public DUUIComposer add(IDUUIDriverComponent<?>... components) throws InvalidXMLException, IOException, SAXException, CompressorException {
        for (IDUUIDriverComponent<?> component : components)
            add(component.build());
        return this;
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

    public void run(AsyncCollectionReader collectionReader, String name) throws Exception {
        
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }

        TypeSystemDescription desc = instantiate_pipeline();

        System.out.println("[Composer] Generating pipeline-dependency graph.");

        Callable<List<JCas>> runPipeline = () -> run_pipeline(name, collectionReader, desc);

        run(name, runPipeline);

    }

    public List<JCas> run(CollectionReader collectionReader, String name) throws Exception {
        
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }

        TypeSystemDescription desc = instantiate_pipeline();

        System.out.println("[Composer] Generating pipeline-dependency graph.");

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
            new DUUIPipelineProfiler(name, _executionPipeline.getGraph()); 
            Instant starttime = Instant.now();
            measureStart("Complete pipeline run");
            result = runPipeline.call();
            measureEnd("Complete pipeline run");
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            // e.printStackTrace();
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

    public void shutdown() throws UnknownHostException, InterruptedException {
        if(!_hasShutdown) {
            _shutdownAtomic.set(true);
            
            _executorService.shutdownNow(); 
            while (!_executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {}
            if (_executorService.isTerminated()) 
                System.out.println("[DUUIComposer] Executor terminated!");

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
        List<Future<?>> tasks = new ArrayList<>();
        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(_minimalTypesystem);
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());

        try { 
            for (DUUIPipelineComponent comp : _pipeline) {
                tasks.add(
                _executorService.submit(() -> {
                    IDUUIDriverInterface driver = _drivers.get(comp.getDriver());
                    String uuid;
                    try {
                        uuid = driver.instantiate(comp, jc, _skipVerification);
                        
                        TypeSystemDescription desc = driver.get_typesystem(uuid);
                        
                        if (desc != null)
                        synchronized (descriptions) {descriptions.add(desc);}
                        
                        Signature signature = driver.get_signature(uuid);
                        
                        synchronized (_instantiatedPipeline) {
                            _instantiatedPipeline.add(new PipelinePart(driver, uuid, signature));
                        }  
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
            }

            for (Future<?> task : tasks) 
                task.get();

            if (_workers > 1)
                _executionPipeline = new DUUIParallelExecutionPipeline(_instantiatedPipeline);
            else _executorService.shutdownNow(); 
            
        }
        catch (Exception e){
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

    private List<JCas> run_pipeline(String name, AsyncCollectionReader reader, TypeSystemDescription desc) 
        throws Exception {

        List<Future<Boolean>> runningComponents = new ArrayList<>();
        Map<Integer, JCas> results = new ConcurrentHashMap<>();

        AtomicInteger d = new AtomicInteger(1);
        Callable<Boolean> component = null; 
        for (String compId : _executionPipeline._executionplan) {
            boolean firstComponent = !reader.isEmpty();
            while(!reader.isEmpty()) {
                // Instantiate JCas.
                String currName = format("%s-%d", name, d.get()); 
                
                JCas jc = JCasFactory.createJCas(desc);
                DUUIPipelineProfiler.add(currName, jc);

                measureStart(currName, "Document retrieval");
                long waitTimeStart = System.nanoTime();
                reader.getNextCAS(jc);
                long waitTimeEnd = System.nanoTime();
                measureEnd(currName, "Document retrieval");

                DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(currName, waitTimeEnd-waitTimeStart,jc);
                // Get component and immediatiately submit to executorService
                // for (String compId : _executionPipeline._executionplan) {
                component = _executionPipeline.getComponentRunner(currName, compId, jc, perf);
                d.incrementAndGet();
                runningComponents.add(_executorService.submit(component));
                // }

                
            }
            
            if (firstComponent) continue;
           
            for (int i = 1; i < d.get(); i++) {
                String currName = format("%s-%d", name, i); 
                component = _executionPipeline.getRunner(currName, compId);

                if (component == null) 
                    throw new RuntimeException(format("[%s][%s] doesn't exist.", currName, compId));
                runningComponents.add(_executorService.submit(component));
            }
        }

        _executorService.shutdown();
        try {
            while (!_executorService.awaitTermination(100*runningComponents.size(), TimeUnit.MILLISECONDS)) {}
            for (Future<Boolean> task : runningComponents) 
                task.get();
        } catch (Exception e) {
            _executorService.shutdownNow();
            throw e; 
        }

        return results.values().stream().collect(Collectors.toList());
    }

    private List<JCas> run_pipeline(String name, CollectionReader reader, TypeSystemDescription desc) 
        throws Exception {

        
        List<Future<Boolean>> runningComponents = new ArrayList<>();
        Map<Integer, JCas> results = new ConcurrentHashMap<>();

        AtomicInteger d = new AtomicInteger(1);
        Callable<Boolean> component = null; 
        for (String compId : _executionPipeline._executionplan) {
            // TODO: Make executionPlan graph height based.
            boolean firstComponent = !reader.hasNext();
            while(!reader.hasNext()) {
                // Instantiate JCas.
                JCas jc = JCasFactory.createJCas(desc);
                long waitTimeStart = System.nanoTime();
                reader.getNext(jc.getCas());
                long waitTimeEnd = System.nanoTime();

                DUUIPipelineDocumentPerformance perf = 
                    new DUUIPipelineDocumentPerformance(
                        name + d.get(), 
                        waitTimeEnd-waitTimeStart,
                        jc);
                // Get component and immediatiately submit to executorService
                component = 
                    _executionPipeline.getComponentRunner(name + d.getAndIncrement(), compId, jc, perf);

                runningComponents.add(_executorService.submit(component));
                
            }
            
            if (firstComponent) continue;
           
            for (int i = 1; i < d.get(); i++) {
                component = _executionPipeline.getRunner(name + i, compId);

                if (component == null) 
                    throw new RuntimeException(format("[%s][%s] doesn't exist.", name + i, compId));
                runningComponents.add(_executorService.submit(component));
            }
        }

        _executorService.shutdown();
        try {
            while (!_executorService.awaitTermination(100*runningComponents.size(), TimeUnit.MILLISECONDS)) {}
            for (Future<Boolean> task : runningComponents) 
                task.get();
        } catch (Exception e) {
            _executorService.shutdownNow();
            throw e; 
        }

        return results.values().stream().collect(Collectors.toList());
    }

    private void run_pipeline(JCas jc, String name, DUUIPipelineDocumentPerformance perf) throws Exception {
        List<Future<Boolean>> workers = _executorService.invokeAll(_executionPipeline.getAllComponentRunners(name, jc, perf));

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
        private final Signature _signature; 

        PipelinePart(IDUUIDriverInterface driver, String uuid) {
            _driver = driver;
            _uuid = uuid;
            _signature = null;
        }

        public PipelinePart(IDUUIDriverInterface driver, String uuid, Signature signature) {
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

        public Signature getSignature() {
            return _signature; 
        }

        public IDUUIDriverInterface getDriver() {
            return _driver;
        }

        public String getUUID() {
            return _uuid;
        }
    }

    public static void main(String[] args) throws Exception {
        // DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
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
            // .withMonitor(new DUUIMonitor("admin", "admin", 8087))
            .withWorkers(10)
            .withSkipVerification(true)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIDockerDriver(), new DUUIRemoteDriver());
        
        composer.add(  
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-ner:latest")
                .withImageFetching().withScale(6).withGPU(true),
            
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-lemmatizer:latest")
                .withImageFetching().withScale(6).withGPU(true),
            
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-morphologizer:latest")
                .withImageFetching().withScale(6).withGPU(true),
            
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-parser:latest")
                .withImageFetching().withScale(6).withGPU(true),
            
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-sentencizer:latest")
                .withImageFetching().withScale(6).withGPU(true),
            
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
                .withImageFetching().withScale(6).withGPU(true)
        );
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

        // String val = "Dies ist ein kleiner Test Text für Abies!";
        // JCas jc = JCasFactory.createJCas();
        // jc.setDocumentLanguage("de");
        // jc.setDocumentText(val);

        // JCas jc2 = JCasFactory.createJCas();
        // jc2.setDocumentLanguage("de");
        // jc2.setDocumentText(val);

        // // Run single document
        // composer.run(jc,"fuchs");
        
        // // composer.withWorkers(1);
        // // composer.run(jc2,"fuchs");
        // composer.shutdown();
        


        // System.out.println(
        //     Objects.equals(JCasUtil.select(jc, TOP.class).stream().count(), JCasUtil.select(jc2, TOP.class).stream().count())
        // );
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
        
        
        CollectionReaderDescription reader = null;
        reader = org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_SOURCE_LOCATION,  "C:\\Users\\davet\\projects\\DockerUnifiedUIMAInterface\\src\\main\\resources\\sample\\**.gz.xmi.gz",
                XmiReader.PARAM_SORT_BY_SIZE, true
        );

        AsyncCollectionReader rd = new AsyncCollectionReader("src\\main\\resources\\sample_splitted\\", ".txt", 1, -1, true, "", true);

        composer.run(rd, "ComponentFirst");
        DUUIPipelineProfiler.printMeasurements();
        composer.shutdown();


  }

} 



