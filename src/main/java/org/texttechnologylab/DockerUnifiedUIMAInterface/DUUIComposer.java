package org.texttechnologylab.DockerUnifiedUIMAInterface;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.pipelineUpdate;

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
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUISimpleMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.IDUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.AdaptiveStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUILinearPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUISemiParallelPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DefaultStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.FixedStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.IDUUIPipelineExecutor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.PoolStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
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
            return _composer._withParallelPipeline.get();
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
    private AtomicBoolean _withParallelPipeline = new AtomicBoolean(false);
    private AtomicBoolean _withSemiParallelPipeline = new AtomicBoolean(false);
    private boolean _batchSynchronized = false;
    private boolean _shared = true;
    private JCasWriter _writer = jCas -> {};

    public static AtomicLong totalurlwait = new AtomicLong(0);
    public static AtomicLong totalannotatorwait = new AtomicLong(0);
    public static AtomicLong totalserializewait = new AtomicLong(0);
    public static AtomicLong totaldeserializewait = new AtomicLong(0);
    public static AtomicLong totalreadwait = new AtomicLong(0);
    public static AtomicLong totalbytestreamwait = new AtomicLong(0);
    public static AtomicLong totalscalingwait = new AtomicLong(0);

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
        ResourceManager.register(Thread.currentThread());
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
        _rm.withMonitor(_monitor);
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
    
    public DUUIComposer withParallelPipeline(PoolStrategy strategy, boolean batchSynchronized) {
        _strategy = strategy; 
        _batchSynchronized = batchSynchronized;
        _withParallelPipeline.set(true);
        return this;
    }
    
    public DUUIComposer withParallelPipeline(PoolStrategy strategy, boolean batchSynchronized, boolean shared) {
        _strategy = strategy; 
        _batchSynchronized = batchSynchronized;
        _shared = shared;
        _withParallelPipeline.set(true);
        return this;
    }
    
    public DUUIComposer withParallelPipeline(PoolStrategy strategy) {
        _strategy = strategy; 
        _withParallelPipeline.set(true);
        return this;
    }

    public DUUIComposer withSemiParallelPipeline(PoolStrategy strategy) {
        _withSemiParallelPipeline.set(true);
        return withParallelPipeline(strategy);
    }
    
    public DUUIComposer withParallelPipeline() {
        _withParallelPipeline.set(true);
        _strategy = new DefaultStrategy(); 
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
            if (_monitor != null)
                new DUUIPipelineProfiler(name, _executionPipeline.getGraph(), (DUUISimpleMonitor) _monitor); 

            _rm.start();
            
            Instant starttime = Instant.now();
            runPipeline.call();
            Instant duration = Instant.now().minusSeconds(starttime.getEpochSecond());

            pipelineUpdate("duration", duration.getEpochSecond() + " s");
            DUUIPipelineProfiler.statusUpdate("FINISHED", format("Run successfully finished: %s", name));
            System.out.printf("FINISHED ANALYSIS: %d s %n", duration.getEpochSecond());
            System.out.printf("URL WAIT %d ms%n", TimeUnit.SECONDS.convert(totalurlwait.get(), TimeUnit.NANOSECONDS));
            System.out.printf("SERIALIZE WAIT %d s%n", TimeUnit.SECONDS.convert(totalserializewait.get(), TimeUnit.NANOSECONDS));
            System.out.printf("ANNOTATOR WAIT %d s%n", TimeUnit.SECONDS.convert(totalannotatorwait.get(), TimeUnit.NANOSECONDS));
            System.out.printf("DESERIALIZE WAIT %d s%n", TimeUnit.SECONDS.convert(totaldeserializewait.get(), TimeUnit.NANOSECONDS));
            System.out.printf("BYTESTREAM WAIT %d s%n", TimeUnit.SECONDS.convert(totalbytestreamwait.get(), TimeUnit.NANOSECONDS));
            System.out.printf("SCALING WAIT %d s%n", TimeUnit.SECONDS.convert(totalscalingwait.get(), TimeUnit.NANOSECONDS));
            System.out.printf("READ WAIT %d s%n", TimeUnit.SECONDS.convert(totalreadwait.get(), TimeUnit.NANOSECONDS));
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            // e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            DUUIPipelineProfiler.statusUpdate("FAILED", format("Fatal exception occured: %s", e));
            catched = e;
        } finally { 
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
        if (_withParallelPipeline.get() && _withSemiParallelPipeline.get()) {
            _executionPipeline = new DUUISemiParallelPipelineExecutor(_instantiatedPipeline);
        } else if (_withParallelPipeline.get()) {
            _executionPipeline = new DUUIParallelPipelineExecutor(_instantiatedPipeline, _shared)
                .withLevelSynchronization(_batchSynchronized);
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
        
        String _title = JCasUtil.select(jc, DocumentMetaData.class)
                .stream().map(meta -> meta.getDocumentTitle()).findFirst().orElseGet(() -> "");

        DUUIPipelineProfiler.documentMetaDataUpdate(name, _title, jc.size());

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
            pipelineUpdate("document_count", readCount.get());
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
            
            pipelineUpdate("document_count", readCount.get());
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
            // .withMonitor(new DUUISimpleMonitor())
            .withStorageBackend(new DUUISqliteStorageBackend("gerparcor_sample1000_smallest"))
        // .withParallelPipeline()
            .withCasPoolMemoryThreshhold(500_000_000)
            // .withSemiParallelPipeline(new AdaptiveStrategy(3, 3))
            .withParallelPipeline(new AdaptiveStrategy(50, 2, 6), 
                true, 
                true
            )
            // .withParallelPipeline(new FixedStrategy(15), true)
            // .withParallelPipeline(new FixedStrategy(10), true)
            .withSkipVerification(true)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIDockerDriver().withContainerPause());
        
        composer.add(  
            // new DUUIDockerDriver.Component("docker.texttechnologylab.org/languagedetection:0.5"),
            new DUUIDockerDriver.Component("tokenizer:latest")//.withScale(2)
                .withImageFetching(),
            new DUUIDockerDriver.Component("sentencizer:latest")//.withScale(2)
                .withImageFetching(),
            new DUUIDockerDriver.Component("parser:latest")
                .withImageFetching()
            // new DUUIDockerDriver.Component("ner:latest")
            //     .withImageFetching(),
            // new DUUIDockerDriver.Component("lemmatizer:latest")
            //     .withImageFetching(),
            // new DUUIDockerDriver.Component("morphologizer:latest")
            //     .withImageFetching(),
            // new DUUIDockerDriver.Component("tagger:latest")
            //     .withImageFetching()
        );

        AsyncCollectionReader rd = new AsyncCollectionReader(
            "gerparcor_sample_smallest1000", 
            ".xmi.gz",
            1,
            100,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.LARGEST,
            "gerparcor_sample_smallest1000",
            false,
            "de",
            100*1024);

        // composer.run(rd, "gerparcor_sample1000_smallest_fixed3scale3");
        // composer.run(rd, "gerparcor_sample1000_smallest_fixed3scale5");
        // composer.run(rd, "gerparcor_sample1000_smallest_fixed5scale3");
        // composer.run(rd, "gerparcor_sample1000_smallest_fixed5scale5");
        // composer.run(rd, "gerparcor_sample1000_smallest_fixed10scale3");
        // composer.run(rd, "gerparcor_sample1000_smallest_fixed10scale5");

        composer.run(rd, "gerparcor_sample1000_smallest_adaptive10scale3");
        // FINISHED ANALYSIS: 188 s
        // URL WAIT 4 ms
        // SERIALIZE WAIT 3 s
        // ANNOTATOR WAIT 231 s
        // DESERIALIZE WAIT 125 s
        // BYTESTREAM WAIT 0 s
        // SCALING WAIT 0 s
        // READ WAIT 162 





        // FINISHED ANALYSIS: 158 s 
        // URL WAIT 551997 ms
        // SERIALIZE WAIT 6 s
        // ANNOTATOR WAIT 1050 s
        // DESERIALIZE WAIT 241 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 138 s
        // RUNNERS WAIT 0 s


        // FINISHED ANALYSIS: 3127 s 
        // URL WAIT 690104 ms
        // SERIALIZE WAIT 643 s
        // ANNOTATOR WAIT 17935 s
        // DESERIALIZE WAIT 9297 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 3112 s
        // RUNNERS WAIT 0 s

        // FINISHED ANALYSIS: 507 s SHARED POOL
        // URL WAIT 263737 ms
        // SERIALIZE WAIT 97 s
        // ANNOTATOR WAIT 2926 s
        // DESERIALIZE WAIT 1308 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 455 s
        // RUNNERS WAIT 0 s
        
        // FINISHED ANALYSIS: 3569 s POOL SIZE 3 SCALE 3 
        // URL WAIT 24 ms
        // SERIALIZE WAIT 589 s
        // ANNOTATOR WAIT 6394 s
        // DESERIALIZE WAIT 3475 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 3470 s
        // RUNNERS WAIT 0 s

        // FINISHED ANALYSIS: 2571 s  POOL SIZE 5 SCALE 3
        // URL WAIT 53117 ms
        // SERIALIZE WAIT 450 s
        // ANNOTATOR WAIT 7370 s
        // DESERIALIZE WAIT 4334 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 2561 s
        // RUNNERS WAIT 0 s

        // FINISHED ANALYSIS: 2151 s POOL SIZE 10 SCALE 3 
        // URL WAIT 1028961 ms
        // SERIALIZE WAIT 637 s
        // ANNOTATOR WAIT 12094 s
        // DESERIALIZE WAIT 5704 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 2080 s
        // RUNNERS WAIT 0 s







        

        // FINISHED ANALYSIS: 531 s SPLIT POOLS 
        // URL WAIT 277385 ms
        // SERIALIZE WAIT 56 s
        // ANNOTATOR WAIT 2582 s
        // DESERIALIZE WAIT 1095 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 429 s
        // RUNNERS WAIT 0 s>


        // composer.run(rd, "gerparcor_sample1000_smallest_adaptive3scale3");
        // FINISHED ANALYSIS: 1700 s 
        // URL WAIT 47 ms
        // SERIALIZE WAIT 273 s
        // ANNOTATOR WAIT 21386 s
        // DESERIALIZE WAIT 4698 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 1686 s
        // RUNNERS WAIT 0 s
        // composer.run(rd, "gerparcor_sample1000_smallest_adaptive3scale5");
        // composer.run(rd, "gerparcor_sample1000_smallest_adaptive5scale3");
        // Scale 3, Max Pool 5, Cas Pool 50
        // FINISHED ANALYSIS: 1821 s 
        // URL WAIT 2521025 ms
        // SERIALIZE WAIT 287 s
        // ANNOTATOR WAIT 22863 s
        // DESERIALIZE WAIT 4916 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 1793 s
        // RUNNERS WAIT 0 s
        // composer.run(rd, "gerparcor_sample1000_smallest_adaptive5scale5");
        // Max Pool 10, Core Pool 1, Adaptive, Cas Pool 50
        // FINISHED ANALYSIS: 3035 s 
        // URL WAIT 725657 ms
        // SERIALIZE WAIT 572 s
        // ANNOTATOR WAIT 11189 s
        // DESERIALIZE WAIT 5036 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 3000 s
        // RUNNERS WAIT 1 s
        // composer.run(rd, "gerparcor_sample1000_smallest_adaptive10scale3");
        // Max Pool 15, Core Pool 1, Adaptive, Cas Pool 50
        // FINISHED ANALYSIS: 1960 s 
        // URL WAIT 10423675 ms
        // SERIALIZE WAIT 255 s
        // ANNOTATOR WAIT 10784 s
        // DESERIALIZE WAIT 4307 s
        // BYTESTREAM WAIT 0 s
        // READ WAIT 1928 s
        // RUNNERS WAIT 0 s
        // composer.run(rd, "gerparcor_sample1000_smallest_adaptive10scale5");


        
        composer.shutdown();
        // composer.add(  
        //     new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-ner:latest")
        //         .withImageFetching().withScale(3).withGPU(true),
            
        //     new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-lemmatizer:latest")
        //         .withImageFetching().withScale(3).withGPU(true),
            
        //     new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-morphologizer:latest")
        //         .withImageFetching().withScale(3).withGPU(true),
            
        //     new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-parser:latest")
        //         .withImageFetching().withScale(3).withGPU(true),
            
        //     new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-sentencizer:latest")
        //         .withImageFetching().withScale(3).withGPU(true),
            
        //     new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-tokenizer:latest")
        //         .withImageFetching().withScale(3).withGPU(true)
        // );

        // String val = "Dies ist ein kleiner Test Text für Abies!";
        // JCas jc = JCasFactory.createJCas();
        // jc.setDocumentLanguage("de");
        // jc.setDocumentText(val);

        // Iterable<Sentence> it = () -> JCasUtil.select(jc, Sentence.class).iterator();

        // for (Sentence sentence : it) {
        //     sentence.
        // }

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
        
        
        // CollectionReaderDescription reader = null;
        // reader = org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription(XmiReader.class,
        //         XmiReader.PARAM_SOURCE_LOCATION,  "C:\\Users\\davet\\projects\\DockerUnifiedUIMAInterface\\src\\main\\resources\\sample\\**.gz.xmi.gz",
        //         XmiReader.PARAM_SORT_BY_SIZE, true
        // );

        

        

  }

} 



