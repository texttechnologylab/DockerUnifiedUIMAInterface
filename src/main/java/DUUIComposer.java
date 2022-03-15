import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.base_cpm.CasDataCollectionReader;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Pipe;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
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

    DUUIWorker(Vector<DUUIComposer.PipelinePart> engineFlow, ConcurrentLinkedQueue<JCas> emptyInstance, ConcurrentLinkedQueue<JCas> loadedInstances, AtomicBoolean shutdown, AtomicInteger error) {
        super();
        _flow = engineFlow;
        _instancesToBeLoaded = emptyInstance;
        _loadedInstances = loadedInstances;
        _shutdown = shutdown;
        _threadsAlive = error;
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

            DUUIEither either = new DUUIEither(object);
            for (DUUIComposer.PipelinePart i : _flow) {
                try {
                    either = i.getDriver().run(i.getUUID(),either);
                } catch (Exception e) {
                    //Ignore errors at the moment
                    e.printStackTrace();
                }
            }
            object.reset();
            _instancesToBeLoaded.add(object);
        }
    }
}


public class DUUIComposer {
    private Map<String, IDUUIDriverInterface> _drivers;
    private Vector<IDUUIPipelineComponent> _pipeline;
    private int _workers;
    public Integer _cas_poolsize;

    private static final String DRIVER_OPTION_NAME = "duuid.composer.driver";

    public DUUIComposer() {
        _drivers = new HashMap<String, IDUUIDriverInterface>();
        _pipeline = new Vector<IDUUIPipelineComponent>();
        _workers = 1;
        _cas_poolsize = null;
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
        _drivers.put(driver.getClass().getCanonicalName().toString(), driver);
        return this;
    }

    public <Y> DUUIComposer add(IDUUIPipelineComponent object, Class<Y> t) {
        object.setOption(DRIVER_OPTION_NAME, t.getCanonicalName().toString());
        IDUUIDriverInterface driver = _drivers.get(t.getCanonicalName().toString());
        if (driver == null) {
            throw new InvalidParameterException(format("No driver %s in the composer installed!", t.getCanonicalName().toString()));
        } else {
            if (!driver.canAccept(object)) {
                throw new InvalidParameterException(format("The driver %s cannot accept %s as input!", t.getCanonicalName().toString(), object.getClass().getCanonicalName().toString()));
            }
        }
        _pipeline.add(object);
        return this;
    }

    public static class PipelinePart {
        private IDUUIDriverInterface _driver;
        private String _uuid;

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

    public void run_async(CollectionReader collectionReader) throws Exception {
        ConcurrentLinkedQueue<JCas> emptyCasDocuments = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<JCas> loadedCasDocuments = new ConcurrentLinkedQueue<>();
        AtomicInteger aliveThreads = new AtomicInteger(0);
        AtomicBoolean shutdown = new AtomicBoolean(false);

        Exception catched = null;

        System.out.printf("Running in asynchronous mode, %d threads at most!\n", _workers);
            if (_cas_poolsize == null) {
                _cas_poolsize = _workers;
            } else {
                if (_cas_poolsize < _workers) {
                    System.err.println("WARNING: Pool size is smaller than the available threads, this is likely a bottleneck.");
                }
            }

        for(int i = 0; i < _cas_poolsize; i++) {
            emptyCasDocuments.add(JCasFactory.createJCas());
        }

        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        try {
            instantiate_pipeline(idPipeline);
            Thread []arr = new Thread[_workers];
            for(int i = 0; i < _workers; i++) {
                System.out.printf("Starting worker thread [%d/%d]\n",i+1,_workers);
                arr[i] = new DUUIWorker(idPipeline,emptyCasDocuments,loadedCasDocuments,shutdown,aliveThreads);
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
                System.out.println("Waiting for threads to finish document processing...");
                Thread.sleep(1000);
            }
            System.out.println("All documents have been processed. Signaling threads to shut down now...");
            shutdown.set(true);

            for(int i = 0; i < arr.length; i++) {
                System.out.printf("Waiting for thread [%d/%d] to shut down\n",i+1,arr.length);
                arr[i].join();
                System.out.printf("Thread %d returned.\n",i);
            }
            System.out.println("All threads returned.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong, shutting down remaining components...");
            catched = e;
        }

        shutdown_pipeline(idPipeline);
        if (catched != null) {
            throw catched;
        }
    }

    public void run(CollectionReaderDescription reader) throws Exception {
        Exception catched = null;
        System.out.println("Instantiation the collection reader...");
        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);
        System.out.println("Instantiated the collection reader.");

        if(_workers == 1) {
            System.out.println("Running in synchronous mode, 1 thread at most!");
            _cas_poolsize = 1;
        }
        else {
            run_async(collectionReader);
            return;
        }

        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        JCas jc = JCasFactory.createJCas();
        try {
            instantiate_pipeline(idPipeline);
            while(collectionReader.hasNext()) {
                collectionReader.getNext(jc.getCas());
                run_pipeline(jc,idPipeline);
                jc.reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong, shutting down remaining components...");
            catched = e;
        }

        shutdown_pipeline(idPipeline);
        if (catched != null) {
            throw catched;
        }
    }

    private void instantiate_pipeline(Vector<PipelinePart> idPipeline) throws Exception {
        for (IDUUIPipelineComponent comp : _pipeline) {
            IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
            idPipeline.add(new PipelinePart(driver, driver.instantiate(comp)));
        }
        System.out.println("");
    }

    private DUUIEither run_pipeline(JCas jc, Vector<PipelinePart> pipeline) throws Exception {
        DUUIEither start = new DUUIEither(jc);

        for (PipelinePart comp : pipeline) {
            start = comp.getDriver().run(comp.getUUID(), start);
        }
        return start;
    }

    private void shutdown_pipeline(Vector<PipelinePart> pipeline) throws Exception {
        for (PipelinePart comp : pipeline) {
            System.out.printf("Shutting down %s...\n", comp.getUUID());
            comp.getDriver().destroy(comp.getUUID());
        }
        System.out.println("Shut down complete.\n");
    }

    public void run(JCas jc) throws Exception {
        Exception catched = null;
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        if(_workers!=1) {
            System.err.println("WARNING: Single document processing runs always single threaded, worker threads are ignored!");
        }

        try {
            instantiate_pipeline(idPipeline);
            DUUIEither start = run_pipeline(jc,idPipeline);

            String cas = start.getAsString();
            System.out.printf("Result %s\n", cas);
            jc = start.getAsJCas();

            System.out.printf("Total number of transforms in pipeline %d\n", start.getTransformSteps());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong, shutting down remaining components...");
            catched = e;
        }
        shutdown_pipeline(idPipeline);
        if (catched != null) {
            throw catched;
        }
    }


    public static void main(String[] args) throws Exception {
        // Use two worker threads, at most 2 concurrent pipelines can run
        DUUIComposer composer = new DUUIComposer().withWorkers(2);

        // Instantiate drivers with options
        DUUILocalDriver driver = new DUUILocalDriver()
                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver();

        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(driver);
        composer.addDriver(remote_driver);
        composer.addDriver(uima_driver);

        // Every component needs a driver which instantiates and runs them
        // Local driver manages local docker container and pulls docker container from remote repositories
        composer.add(new DUUILocalDriver.Component("new:latest", true)
                        .withScale(2)
                        .withRunningAfterDestroy(false)
                , DUUILocalDriver.class);

        // Remote driver handles all pure URL endpoints
        composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9714"),
                DUUIRemoteDriver.class);

        // UIMA Driver handles all native UIMA Analysis Engine Descriptions
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class)
        ).withScale(2), DUUIUIMADriver.class);


        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        // Run single document
        composer.run(jc);

        // Run Collection Reader
        composer.run(createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, "test_corpora/**.txt",
                TextReader.PARAM_LANGUAGE, "en"));
    }
}