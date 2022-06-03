package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XmlCasSerializer;
import org.luaj.vm2.Globals;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIMonitor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.annotation.type.Taxon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

class DUUIWorker extends Thread {
    Vector<DUUIComposer.PipelinePart> _flow;
    ConcurrentLinkedQueue<JCas> _instancesToBeLoaded;
    ConcurrentLinkedQueue<JCas> _loadedInstances;
    AtomicInteger _threadsAlive;
    AtomicBoolean _shutdown;
    IDUUIStorageBackend _backend;
    String _runKey;

    DUUIWorker(Vector<DUUIComposer.PipelinePart> engineFlow, ConcurrentLinkedQueue<JCas> emptyInstance, ConcurrentLinkedQueue<JCas> loadedInstances, AtomicBoolean shutdown, AtomicInteger error,
               IDUUIStorageBackend backend, String runKey) {
        super();
        _flow = engineFlow;
        _instancesToBeLoaded = emptyInstance;
        _loadedInstances = loadedInstances;
        _shutdown = shutdown;
        _threadsAlive = error;
        _backend = backend;
        _runKey = runKey;
    }

    @Override
    public void run() {
        _threadsAlive.addAndGet(1);
        while(true) {

            JCas object = null;
            while(object == null) {
                object = _loadedInstances.poll();

                if(_shutdown.get()) {
                    return;
                }
            }

            DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(_runKey,object);
            for (DUUIComposer.PipelinePart i : _flow) {
                try {
                    i.getDriver().run(i.getUUID(),object,perf);
                } catch (Exception e) {
                    //Ignore errors at the moment
                    e.printStackTrace();
                }
            }
            object.reset();
            _instancesToBeLoaded.add(object);
            if(_backend!=null) {
                _backend.addMetricsForDocument(perf);
            }
        }
    }
}


public class DUUIComposer {
    private final Map<String, IDUUIDriverInterface> _drivers;
    private final Vector<IDUUIPipelineComponent> _pipeline;

    private int _workers;
    public Integer _cas_poolsize;
    private DUUILuaContext _context;
    private DUUIMonitor _monitor;
    private IDUUIStorageBackend _storage;

    private static final String DRIVER_OPTION_NAME = "duuid.composer.driver";
    public static final String COMPONENT_COMPONENT_UNIQUE_KEY = "duuid.storage.componentkey";

    public static final String V1_COMPONENT_ENDPOINT_PROCESS = "/v1/process";
    public static final String V1_COMPONENT_ENDPOINT_TYPESYSTEM = "/v1/typesystem";
    public static final String V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER = "/v1/communication_layer";



    public DUUIComposer() {
        _drivers = new HashMap<>();
        _pipeline = new Vector<>();
        _workers = 1;
        _cas_poolsize = null;
        Globals globals = JsePlatform.standardGlobals();
        _context = new DUUILuaContext();
        _monitor = null;
        _storage = null;
        System.out.println("[Composer] Initialised LUA scripting layer with version "+ globals.get("_VERSION"));
    }

    public DUUIComposer withMonitor(DUUIMonitor monitor) throws UnknownHostException, InterruptedException {
        _monitor = monitor;
        _monitor.setup();
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

    public IDUUIPipelineComponent addFromBackend(String id) {
        if(_storage == null) {
            throw new RuntimeException("[DUUIComposer] No storage backend specified but trying to load component from it!");
        }
        _pipeline.add(_storage.loadComponent(id));
        IDUUIDriverInterface driver = _drivers.get(_pipeline.lastElement().getOption(DUUIComposer.DRIVER_OPTION_NAME));
        if (driver == null) {
            throw new InvalidParameterException(format("[DUUIComposer] No driver %s in the composer installed!", _pipeline.lastElement().getOption(DUUIComposer.DRIVER_OPTION_NAME)));
        }
        return _pipeline.lastElement();
    }

    public <Y> DUUIComposer add(IDUUIPipelineComponent object, Class<Y> t) {
        object.setOption(DRIVER_OPTION_NAME, t.getCanonicalName());
        IDUUIDriverInterface driver = _drivers.get(t.getCanonicalName());
        if (driver == null) {
            throw new InvalidParameterException(format("[DUUIComposer] No driver %s in the composer installed!", t.getCanonicalName()));
        } else {
            if (!driver.canAccept(object)) {
                throw new InvalidParameterException(format("[DUUIComposer] The driver %s cannot accept %s as input!", t.getCanonicalName(), object.getClass().getCanonicalName()));
            }
        }
        _pipeline.add(object);
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


    private void run_async(CollectionReader collectionReader, String name) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<JCas> loadedCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        AtomicBoolean shutdown = new AtomicBoolean(false);

        Exception catched = null;

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);
        Vector<PipelinePart> idPipeline = new Vector<>();

        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            Instant starttime = Instant.now();
            TypeSystemDescription desc = instantiate_pipeline(idPipeline);
            if (_cas_poolsize == null) {
                _cas_poolsize = _workers;
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
                arr[i] = new DUUIWorker(idPipeline,emptyCasDocuments,loadedCasDocuments,shutdown,aliveThreads,_storage,name);
                arr[i].start();
            }
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
            shutdown.set(true);

            for(int i = 0; i < arr.length; i++) {
                System.out.printf("[Composer] Waiting for thread [%d/%d] to shut down\n",i+1,arr.length);
                arr[i].join();
                System.out.printf("[Composer] Thread %d returned.\n",i);
            }
            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
            System.out.println("[Composer] All threads returned.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }

        shutdown_pipeline(idPipeline);
        if (catched != null) {
            throw catched;
        }
    }

    public void run(CollectionReaderDescription reader) throws Exception {
        run(reader,null);
    }

    public Vector<IDUUIPipelineComponent> getPipeline() {
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

        Vector<PipelinePart> idPipeline = new Vector<>();
        Thread shutdownHook = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    shutdown_pipeline(idPipeline);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            Instant starttime = Instant.now();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            TypeSystemDescription desc = instantiate_pipeline(idPipeline);
            JCas jc = JCasFactory.createJCas(desc);
            while(collectionReader.hasNext()) {
                collectionReader.getNext(jc.getCas());
                run_pipeline(name,jc,idPipeline);
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

        shutdown_pipeline(idPipeline);
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        if (catched != null) {
            throw catched;
        }
    }

    private TypeSystemDescription instantiate_pipeline(Vector<PipelinePart> idPipeline) throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        DocumentMetaData dmd = DocumentMetaData.create(jc);
        dmd.setDocumentId("removeMe");
        dmd.setDocumentUri("/tmp/removeMe");
        dmd.setDocumentBaseUri("/tmp/");

        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        try {
            for (IDUUIPipelineComponent comp : _pipeline) {
                IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
                String uuid = driver.instantiate(comp, jc);

                TypeSystemDescription desc = driver.get_typesystem(uuid);
                if (desc != null) {
                    descriptions.add(desc);
                }
                idPipeline.add(new PipelinePart(driver, uuid));
            }
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private JCas run_pipeline(String name, JCas jc, Vector<PipelinePart> pipeline) throws Exception {
        DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name,jc);
        for (PipelinePart comp : pipeline) {
            comp.getDriver().run(comp.getUUID(), jc, perf);
        }

        if(_storage!=null) {
            _storage.addMetricsForDocument(perf);
        }

        return jc;
    }

    private void shutdown_pipeline(Vector<PipelinePart> pipeline) throws Exception {
        for (PipelinePart comp : pipeline) {
            System.out.printf("[Composer] Shutting down %s...\n", comp.getUUID());
            comp.getDriver().destroy(comp.getUUID());
        }
        System.out.println("[Composer] Shut down complete.");

        if(_monitor!=null) {
            System.out.printf("[Composer] Visit %s to view the data.\n",_monitor.generateURL());
        }
    }

    public void printConcurrencyGraph() throws Exception {
        Exception catched = null;
        Vector<PipelinePart> idPipeline = new Vector<>();
        try {
            instantiate_pipeline(idPipeline);
            System.out.printf("[Composer]: CAS Pool size %d\n", Objects.requireNonNullElseGet(_cas_poolsize, () -> _workers));
            System.out.printf("[Composer]: Worker threads %d\n", _workers);
            for (PipelinePart comp : idPipeline) {
                comp.getDriver().printConcurrencyGraph(comp.getUUID());
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            System.out.println(idPipeline+"\t[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }
        shutdown_pipeline(idPipeline);
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
        Vector<PipelinePart> idPipeline = new Vector<>();
        if(_workers!=1) {
            System.err.println("[Composer] WARNING: Single document processing runs always single threaded, worker threads are ignored!");
        }

        Thread shutdownHook = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    shutdown_pipeline(idPipeline);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        try {
            if(_storage!=null) {
                _storage.addNewRun(name,this);
            }
            Instant starttime = Instant.now();

            Runtime.getRuntime().addShutdownHook(shutdownHook);
            instantiate_pipeline(idPipeline);
            JCas start = run_pipeline(name,jc,idPipeline);

            if(_storage!=null) {
                _storage.finalizeRun(name,starttime,Instant.now());
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[Composer] Something went wrong, shutting down remaining components...");
            catched = e;
        }
        shutdown_pipeline(idPipeline);
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        if (catched != null) {
            throw catched;
        }
    }

    public int getWorkerCount() {
        return _workers;
    }


    public void shutdown() throws UnknownHostException {
        if(_monitor!=null) {
            _monitor.shutdown();
        }
        else if(_storage!=null) {
            _storage.shutdown();
        }
    }


    public static void main(String[] args) throws Exception {

        DUUILuaContext ctx = new DUUILuaContext().withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        DUUIComposer composer = new DUUIComposer()
        //        .withStorageBackend(new DUUIArangoDBStorageBackend("password",8888))
                .withLuaContext(ctx);

        // Instantiate drivers with options
        DUUIDockerDriver driver = new DUUIDockerDriver()
                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();

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
      /*  composer.add(new DUUILocalDriver.Component("java_segmentation:latest")

        composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)),
                DUUIUIMADriver.class);
//        composer.add(new DUUILocalDriver.Component("java_segmentation:latest")
//                        .withScale(1)
//                , DUUILocalDriver.class);
        composer.add(new DUUIDockerDriver.Component("gnfinder:0.1")
                        .withScale(1)
                , DUUIDockerDriver.class);
//        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
//                        .withScale(1)
//                , DUUIRemoteDriver.class);*/

        // Remote driver handles all pure URL endpoints
        /*composer.add(new DUUIUIMADriver.Component(AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class))
                        .withScale(1),
                org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver.class);

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

        composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.Component("http://127.0.0.1:9715")
                        .withScale(1),
                org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver.class);

       // ByteArrayInputStream stream;
       // stream.read

        String val2 = "Dies ist ein kleiner Test Text für Abies!";
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("de");
        jc.setDocumentText(val2);

        // Run single document
        composer.run(jc,"Praktikum2");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(jc.getCas(),out);
        System.out.println(new String(out.toByteArray()));

        JCasUtil.select(jc, Taxon.class).forEach(t->{
            System.out.println(t);
        });

        /*
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_token_only.lua").toURI()));
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote");
        OutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
        System.out.println(out.toString());

        OutputStream out2 = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(),out2);
        System.out.println(out2.toString());*/

        // Run Collection Reader

        /*composer.run(createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, "test_corpora/**.txt",
                TextReader.PARAM_LANGUAGE, "en"),"next11");*/
        composer.shutdown();
  }
}
