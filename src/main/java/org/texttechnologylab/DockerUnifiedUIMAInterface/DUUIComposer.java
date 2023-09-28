package org.texttechnologylab.DockerUnifiedUIMAInterface;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatns;


import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.dkpro.core.io.xmi.XmiReader;
import org.luaj.vm2.Globals;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUILinearPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUISemiParallelPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.IDUUIPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy.AdaptiveStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy.DefaultStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy.FixedStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy.PoolStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIConsoleMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUISimpleMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.IDUUIMonitor;
import org.xml.sax.SAXException;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

public class DUUIComposer {

    public static interface JCasWriter extends Consumer<JCas> {
    }

    public static class Config {
        static DUUIComposer _composer; 
        
        public static int workers() {
            return _composer._workers;
        }

        public static boolean skipVerification() {
            return _composer._skipVerification;
        }

        public static IDUUIStorageBackend storage() {
            return _composer._storage; 
        }

        public static IDUUIMonitor monitor()   {
            return _composer._monitor;
        }

        public static PoolStrategy strategy() {
            return _composer._strategy; 
        }

        public static boolean isParallel() {
            return _composer._withParallelPipeline;
        }

        public static void write(JCas jc) {
            _composer._writer.accept(jc);
        }
    }

    private final Map<String, IDUUIDriver> _drivers;
    private final Vector<DUUIPipelineComponent> _pipeline;

    private int _workers;
    public Integer _cas_poolsize;
    private PoolStrategy _strategy = new DefaultStrategy(); 

    private DUUILuaContext _context;
    private IDUUIMonitor _monitor;
    private IDUUIStorageBackend _storage;
    private boolean _skipVerification;

    private Vector<PipelinePart> _instantiatedPipeline;
    private IDUUIPipelineExecutor _executionPipeline;
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

    ResourceManager _rm; 
    public static List<IDUUIConnectionHandler> _clients = new ArrayList<>();
    private boolean _connection_open = false;

    private TypeSystemDescription _minimalTypesystem;
    private boolean _withParallelPipeline = false;
    private boolean _withSemiParallelPipeline = false;
    private boolean _levelSynchronized = false;
    private int _maxLevelWidth = Integer.MAX_VALUE;
    private JCasWriter _writer = jCas -> {};

    public static AtomicLong totalurlwait = new AtomicLong(0);
    public static AtomicLong totalannotatorwait = new AtomicLong(0);
    public static AtomicLong totalserializewait = new AtomicLong(0);
    public static AtomicLong totaldeserializewait = new AtomicLong(0);
    public static AtomicLong totalreadwait = new AtomicLong(0);
    public static AtomicLong totalscalingwait = new AtomicLong(0);
    public static AtomicLong totalafterworkerwait = new AtomicLong(0);
    public static AtomicLong totalrm = new AtomicLong(0);

    public DUUIComposer() throws URISyntaxException {
        Config._composer = this; 
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
        _rm = ResourceManager.getInstance();
        _minimalTypesystem = TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/types/reproducibleAnnotations.xml").toURI().toString());
        System.out.println("[Composer] Initialised LUA scripting layer with version "+ globals.get("_VERSION"));

        DUUIComposer that = this;
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

    public DUUIComposer withJCasWriter(JCasWriter writer) {
        _writer = writer;
        return this;
    }

    public DUUIComposer withMonitor(IDUUIMonitor monitor) throws Exception {
        _monitor = monitor;
        _monitor.setup();
        if (_monitor instanceof IDUUIResource)
            ResourceManager.register((IDUUIResource)monitor);
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

    public DUUIComposer withResourceManager(double jCasMemoryThreshholdPercentage) {
        if (jCasMemoryThreshholdPercentage <= 0.0 || jCasMemoryThreshholdPercentage >= 100.0) {
            throw new IllegalArgumentException(
                format("A valid percentage must be supplied as a memory threshold: %s", 
                jCasMemoryThreshholdPercentage)
            );
        }

        _rm = new ResourceManager(jCasMemoryThreshholdPercentage, -1L);
        return this;
    }

    public DUUIComposer withCasPoolMemoryThreshhold(long jCasMemoryThreshholdBytes) {
        if (jCasMemoryThreshholdBytes <= 0 || jCasMemoryThreshholdBytes >= Runtime.getRuntime().maxMemory()) {
            throw new IllegalArgumentException(
                format("A valid number must be supplied as a memory threshold in bytes: %s", 
                jCasMemoryThreshholdBytes)
            );
        }
        _rm = new ResourceManager(-1, jCasMemoryThreshholdBytes);
        return this;
    }
    
    public DUUIComposer withParallelPipeline(PoolStrategy strategy, boolean levelSynchronized, int maxLevelWidth) {
        if (maxLevelWidth < 1)
            throw new IllegalArgumentException("The level width has to be greater than 1.");
        
        _strategy = strategy; 
        _levelSynchronized = levelSynchronized;
        _withParallelPipeline = true;
        _withSemiParallelPipeline = false;
        _maxLevelWidth = maxLevelWidth;
        return this;
    }
    
    public DUUIComposer withParallelPipeline(PoolStrategy strategy) {
        _strategy = strategy; 
        _withParallelPipeline = true;
        return this;
    }

    public DUUIComposer withSemiParallelPipeline(int casPoolSize, int threadPoolSize) {
        _withSemiParallelPipeline = true;
        return withParallelPipeline(new AdaptiveStrategy(casPoolSize, threadPoolSize, threadPoolSize));
    }

    public DUUIComposer withWorkers(int workers) {
        _workers = workers;
        return this;
    }

    public DUUIComposer withOpenConnection(boolean open) {
        _connection_open = open;
        return this;
    }

    public DUUIComposer addDriver(IDUUIDriver driver) {
        driver.setLuaContext(_context);
        _drivers.put(driver.getClass().getCanonicalName(), driver);
        if (driver instanceof IDUUIResource) 
            ResourceManager.register((IDUUIResource)driver);
        return this;
    }

    public DUUIComposer addDriver(IDUUIDriver... drivers) {
        for (IDUUIDriver driver : drivers) {
            driver.setLuaContext(_context);
            _drivers.put(driver.getClass().getCanonicalName(), driver);
            if (driver instanceof IDUUIResource) 
                ResourceManager.register((IDUUIResource)driver);
        }
        return this;
    }

   /*public IDUUIPipelineComponent addFromBackend(String id) {
        if(_storage == null) {
            throw new RuntimeException("[DUUIComposer] No storage backend specified but trying to load component from it!");
        }
        _pipeline.add(_storage.loadComponent(id));
        IDUUIDriver driver = _drivers.get(_pipeline.lastElement().getOption(DUUIComposer.DRIVER_OPTION_NAME));
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
        IDUUIDriver driver = _drivers.get(object.getDriver());
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

    public Vector<DUUIPipelineComponent> getPipeline() {
        return _pipeline;
    }

    public DUUIComposer resetPipeline() {
        _pipeline.clear();
        return this;
    }

    public void run(CollectionReaderDescription reader) throws Exception {
        run(reader,null);
    }

    public void run(CollectionReaderDescription reader, String name) throws Exception {

        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }
        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);
        
        System.out.println("[Composer] Instantiated the collection reader.");

        run(collectionReader, name);
    }

    public void run(CollectionReader collectionReader, String name) throws Exception {
        
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }

        TypeSystemDescription desc = instantiate_pipeline();

        System.out.println("[Composer] Generating pipeline-dependency graph.");

        Callable<Void> runPipeline = () -> {run_pipeline(name, collectionReader, desc); return null;};

        run(name, runPipeline);

    }

    public void run(AsyncCollectionReader collectionReader, String name) throws Exception {
        
        if(_storage!= null && name == null) {
            throw new RuntimeException("[Composer] When a storage backend is specified a run name is required, since it is the primary key");
        }

        TypeSystemDescription desc = instantiate_pipeline();

        System.out.println("[Composer] Generating pipeline-dependency graph.");

        Callable<Void> runPipeline = () -> {run_pipeline(name, collectionReader, desc); return null;};

        run(name, runPipeline);

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
        
        Callable<Void> runPipeline = () -> {run_pipeline(name, jc); return null;};

        run(name, runPipeline);
    
    }

    private void run(String name, Callable<Void> runPipeline) throws Exception {

        Exception catched = null;
        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }

            _rm.start();
            Instant starttime = Instant.now();
            runPipeline.call();
            Instant duration = Instant.now().minusSeconds(starttime.getEpochSecond());
            System.out.printf("FINISHED ANALYSIS: %d s %n", duration.getEpochSecond()); 
            System.out.printf("URL WAIT %s%n", formatns(DUUIComposer.totalurlwait.getAndSet(0l)));
            System.out.printf("SERIALIZE WAIT %s%n", formatns(DUUIComposer.totalserializewait.getAndSet(0l)));
            System.out.printf("ANNOTATOR WAIT %s%n", formatns(DUUIComposer.totalannotatorwait.getAndSet(0l)));
            System.out.printf("DESERIALIZE WAIT %s%n", formatns(DUUIComposer.totaldeserializewait.getAndSet(0l)));
            System.out.printf("SCALING WAIT %s%n", formatns(DUUIComposer.totalscalingwait.getAndSet(0l)));
            System.out.printf("AFTER WORKER WAIT %s%n", formatns(DUUIComposer.totalafterworkerwait.getAndSet(0l))); 
            System.out.printf("READ WAIT %s%n", formatns(DUUIComposer.totalreadwait.getAndSet(0l))); 
            System.out.printf("RESOURCE MANAGER TOTAL %s%n", formatns(DUUIComposer.totalrm.getAndSet(0l))); 
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            // e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        } finally { 
            _rm.interrupt();
            _rm.finishManager();
        }

        /** shutdown **/
        // shutdown_pipeline();
        if (catched != null) {
            shutdown();
            throw catched;
        }
    }

    public void shutdown() throws UnknownHostException, InterruptedException {
        if(!_hasShutdown) {
            _shutdownAtomic.set(true);
            
            _rm.finishManager();
            if (_executionPipeline != null) _executionPipeline.destroy();
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
            for (IDUUIDriver driver : _drivers.values()) {
                driver.shutdown();
            }

            _hasShutdown = true;
        }
        else {
            System.out.println("Skipped shutdown since it already happened!");
        }
    }

    private TypeSystemDescription instantiate_pipeline() throws Exception {
        _hasShutdown = false;
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText("Hallo Welt!");

        if(_skipVerification) {
            System.out.println("[Composer] Running without verification, no process calls will be made during initialization!");
        }

        Collection<TypeSystemDescription> descriptions = new ConcurrentLinkedQueue<>();
        descriptions.add(_minimalTypesystem);
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());

        // Initialization
        for (DUUIPipelineComponent comp : _pipeline) {
            IDUUIDriver driver = _drivers.get(comp.getDriver());
            String uuid = driver.instantiate(comp, jc, _skipVerification);
            Signature signature = driver.get_signature(uuid);
            TypeSystemDescription desc = driver.get_typesystem(uuid);
            
            if (desc != null) descriptions.add(desc);
            _instantiatedPipeline.add(new PipelinePart(driver, uuid, signature, comp));
        }

        // Pipeline ordering. 
        if (_withParallelPipeline && _withSemiParallelPipeline) {
            _executionPipeline = new DUUISemiParallelPipelineExecutor(_instantiatedPipeline);
        } else if (_withParallelPipeline) {
            _executionPipeline = new DUUIParallelPipelineExecutor(_instantiatedPipeline, _maxLevelWidth)
                .withLevelSynchronization(_levelSynchronized);
        } else {
            _executionPipeline = new DUUILinearPipelineExecutor(_instantiatedPipeline);

        }
        
        if(descriptions.size() > 1) {
            return CasCreationUtils.mergeTypeSystems(descriptions);
        }
        else if(descriptions.size() == 1) {
            return descriptions.stream().findFirst().get();
        }
        else {
            return TypeSystemDescriptionFactory.createTypeSystemDescription();
        }
    }

    private void run_pipeline(String name, JCas jc) throws Exception {

        DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name, 0, jc);

        _executionPipeline.run(name, jc, perf);
        _executionPipeline.shutdown();

        if(_storage!=null) {
            _storage.addMetricsForDocument(perf);
        }
    }

    private void run_pipeline(String name, AsyncCollectionReader reader, TypeSystemDescription desc) 
        throws Exception {

        _rm.initialiseCasPool(_strategy, desc);
        // ToDo: Insert verfication step here to precompute optimal parameters
        AtomicInteger readCount = new AtomicInteger(1);
        while(!reader.isEmpty()) { 
            String currName = format("%s-%d", name, readCount.get()); 

            long waitTimeStart = System.nanoTime();
            JCas jc = _rm.takeCas();
            reader.getNextCAS(jc);
            long waitTimeEnd = System.nanoTime();
            System.out.printf("[%s] It took %d ms for getting new CAS. \n", 
                Thread.currentThread().getName(), 
                TimeUnit.MILLISECONDS.convert(waitTimeEnd-waitTimeStart, TimeUnit.NANOSECONDS));
            totalreadwait.addAndGet(waitTimeEnd-waitTimeStart);
            DUUIPipelineDocumentPerformance perf =
                new DUUIPipelineDocumentPerformance(currName, waitTimeEnd-waitTimeStart,jc);

            _executionPipeline.run(currName, jc, perf);
            readCount.incrementAndGet();
        }


        _executionPipeline.shutdown();
    }

    private void run_pipeline(String name, CollectionReader reader, TypeSystemDescription desc) 
        throws Exception {

        _rm.initialiseCasPool(_strategy, desc);
        AtomicInteger readCount = new AtomicInteger(1);
        while(!reader.hasNext()) { 
            String currName = format("%s-%d", name, readCount.get()); 

            long waitTimeStart = System.nanoTime();
            JCas jc = _rm.takeCas();
            reader.getNext(jc.getCas());
            long waitTimeEnd = System.nanoTime();

            DUUIPipelineDocumentPerformance perf =
                new DUUIPipelineDocumentPerformance(currName, waitTimeEnd-waitTimeStart,jc);

            _executionPipeline.run(currName, jc, perf);
            readCount.incrementAndGet();
        }

        _executionPipeline.shutdown();            
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
            // .withMonitor(new DUUIConsoleMonitor())
            .withStorageBackend(new DUUISqliteStorageBackend("gerparcor_sample1000_smallest"))
        // .withParallelPipeline()
            .withCasPoolMemoryThreshhold(500_000_000)
            // .withSemiParallelPipeline(new AdaptiveStrategy(3, 3))
            .withParallelPipeline(
                new AdaptiveStrategy(30, 4, 4), 
                true, 
                1
            )
            // .withParallelPipeline(new FixedStrategy(15), true)
            // .withParallelPipeline(new FixedStrategy(10), true)
            .withSkipVerification(true)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIDockerDriver().withContainerPause());
        
        composer.add(  
            new DUUIDockerDriver.Component("tokenizer:latest")//.withScale(2)
                .withImageFetching(),
            new DUUIDockerDriver.Component("sentencizer:latest")//.withScale(2)
                .withImageFetching(),
            new DUUIDockerDriver.Component("parser:latest")
                .withImageFetching(),
            new DUUIDockerDriver.Component("ner:latest")
                .withImageFetching(),
            new DUUIDockerDriver.Component("lemmatizer:latest")
                .withImageFetching(),
            new DUUIDockerDriver.Component("morphologizer:latest")
                .withImageFetching(),
            new DUUIDockerDriver.Component("tagger:latest")
                .withImageFetching()
        );

        AsyncCollectionReader rd = new AsyncCollectionReader(
            "gerparcor_sample_smallest1000", 
            ".xmi.gz",
            1,
            500,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.LARGEST,
            "gerparcor_sample_smallest1000",
            false,
            "de",
            100*1024);

        composer.run(rd, "gerparcor_sample1000_smallest_adaptive10scale3");
        System.out.printf("URL WAIT %d ms%n", TimeUnit.SECONDS.convert(totalurlwait.get(), TimeUnit.NANOSECONDS));
        System.out.printf("SERIALIZE WAIT %d s%n", TimeUnit.SECONDS.convert(totalserializewait.get(), TimeUnit.NANOSECONDS));
        System.out.printf("ANNOTATOR WAIT %d s%n", TimeUnit.SECONDS.convert(totalannotatorwait.get(), TimeUnit.NANOSECONDS));
        System.out.printf("DESERIALIZE WAIT %d s%n", TimeUnit.SECONDS.convert(totaldeserializewait.get(), TimeUnit.NANOSECONDS));
        System.out.printf("SCALING WAIT %d s%n", TimeUnit.SECONDS.convert(totalscalingwait.get(), TimeUnit.NANOSECONDS));
        System.out.printf("READ WAIT %d s%n", TimeUnit.SECONDS.convert(totalreadwait.get(), TimeUnit.NANOSECONDS));
        // 50, 4, 4, shared, synchronized, non-squished 
        // FINISHED ANALYSIS: 2463 s
        // URL WAIT 487 ms
        // SERIALIZE WAIT 273 s
        // ANNOTATOR WAIT 6320 s
        // DESERIALIZE WAIT 2374 s
        // BYTESTREAM WAIT 0 s
        // SCALING WAIT 0 s
        // READ WAIT 2308 s
        
        // 50, 4, 4, shared, synchronized, squished-1 
        // FINISHED ANALYSIS: 2347 s 
        // URL WAIT 521 ms
        // SERIALIZE WAIT 60 s
        // ANNOTATOR WAIT 6087 s
        // DESERIALIZE WAIT 2498 s
        // BYTESTREAM WAIT 0 s
        // SCALING WAIT 0 s
        // READ WAIT 2185 s
        
        // 50, 1, 4, shared, synchronized, squished-1 
        // FINISHED ANALYSIS: 2494 s 
        // URL WAIT 1847 ms
        // SERIALIZE WAIT 57 s
        // ANNOTATOR WAIT 5547 s
        // DESERIALIZE WAIT 2257 s
        // BYTESTREAM WAIT 0 s
        // SCALING WAIT 0 s
        // READ WAIT 2267 s
        
        // 100, 1, 4, shared, synchronized, squished-1 
        // FINISHED ANALYSIS: 2724 s 
        // URL WAIT 677 ms
        // SERIALIZE WAIT 62 s
        // ANNOTATOR WAIT 7159 s
        // DESERIALIZE WAIT 2860 s
        // BYTESTREAM WAIT 0 s
        // SCALING WAIT 0 s
        // READ WAIT 2345 s
        
        // 30, 1, 4, shared, synchronized, squished-1 
        // FINISHED ANALYSIS: 2723 s 
        // URL WAIT 898 ms
        // SERIALIZE WAIT 61 s
        // ANNOTATOR WAIT 6810 s
        // DESERIALIZE WAIT 2699 s
        // BYTESTREAM WAIT 0 s
        // SCALING WAIT 0 s
        // READ WAIT 2641 s

        composer.shutdown();
        
        // String val = "Dies ist ein kleiner Test Text für Abies!";
        // JCas jc = JCasFactory.createJCas();
        // jc.setDocumentLanguage("de");
        // jc.setDocumentText(val);
        
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

        

        

  }

} 



