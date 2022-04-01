package org.texttechnologylab.DockerUnifiedUIMAInterface;

import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.commons.compress.archivers.zip.StreamCompressor;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.impl.TypeSystemUtils;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.xml.sax.SAXException;
import javax.sql.rowset.spi.XmlWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
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
    String _compressionMethod;

    DUUIWorker(Vector<DUUIComposer.PipelinePart> engineFlow, ConcurrentLinkedQueue<JCas> emptyInstance, ConcurrentLinkedQueue<JCas> loadedInstances, AtomicBoolean shutdown, AtomicInteger error, String compression) {
        super();
        _flow = engineFlow;
        _instancesToBeLoaded = emptyInstance;
        _loadedInstances = loadedInstances;
        _shutdown = shutdown;
        _threadsAlive = error;
        _compressionMethod = compression;
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

            DUUIEither either = null;
            try {
                either = new DUUIEither(object, _compressionMethod);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (CompressorException e) {
                e.printStackTrace();
            }
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
    private String _compressionMethod;

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

    public DUUIComposer withCompressionMethod(String method) {
        _compressionMethod = method;
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

        System.out.printf("[Composer] Running in asynchronous mode, %d threads at most!\n", _workers);
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();

        try {
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
                arr[i] = new DUUIWorker(idPipeline,emptyCasDocuments,loadedCasDocuments,shutdown,aliveThreads,_compressionMethod);
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
        Exception catched = null;
        System.out.println("[Composer] Instantiation the collection reader...");
        CollectionReader collectionReader = CollectionReaderFactory.createReader(reader);
        System.out.println("[Composer] Instantiated the collection reader.");

        if(_workers == 1) {
            System.out.println("[Composer] Running in synchronous mode, 1 thread at most!");
            _cas_poolsize = 1;
        }
        else {
            run_async(collectionReader);
            return;
        }

        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        try {
            TypeSystemDescription desc = instantiate_pipeline(idPipeline);
            JCas jc = JCasFactory.createJCas(desc);
            while(collectionReader.hasNext()) {
                collectionReader.getNext(jc.getCas());
                run_pipeline(jc,idPipeline);
                jc.reset();
            }
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

    private TypeSystemDescription instantiate_pipeline(Vector<PipelinePart> idPipeline) throws Exception {
        List<TypeSystemDescription> descriptions = new LinkedList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        for (IDUUIPipelineComponent comp : _pipeline) {
            IDUUIDriverInterface driver = _drivers.get(comp.getOption(DRIVER_OPTION_NAME));
            String uuid = driver.instantiate(comp);

            TypeSystemDescription desc = driver.get_typesystem(uuid);
            if(desc!=null) {
                descriptions.add(desc);
            }
            idPipeline.add(new PipelinePart(driver, driver.instantiate(comp)));
        }
        System.out.println("");
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private DUUIEither run_pipeline(JCas jc, Vector<PipelinePart> pipeline) throws Exception {
        DUUIEither start = new DUUIEither(jc,_compressionMethod);

        for (PipelinePart comp : pipeline) {
            start = comp.getDriver().run(comp.getUUID(), start);
        }
        return start;
    }

    private void shutdown_pipeline(Vector<PipelinePart> pipeline) throws Exception {
        for (PipelinePart comp : pipeline) {
            System.out.printf("[Composer] Shutting down %s...\n", comp.getUUID());
            comp.getDriver().destroy(comp.getUUID());
        }
        System.out.println("[Composer] Shut down complete.\n");
    }

    public void printConcurrencyGraph() throws Exception {
        Exception catched = null;
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        try {
            instantiate_pipeline(idPipeline);
            if(_cas_poolsize!=null) {
                System.out.printf("[Composer]: CAS Pool size %d\n", _cas_poolsize);
            }
            else {
                System.out.printf("[Composer]: CAS Pool size %d\n", _workers);
            }
            System.out.printf("[Composer]: Worker threads %d\n", _workers);
            for (PipelinePart comp : idPipeline) {
                comp.getDriver().printConcurrencyGraph(comp.getUUID());
            }
            System.out.println("");
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
        Exception catched = null;
        Vector<PipelinePart> idPipeline = new Vector<PipelinePart>();
        if(_workers!=1) {
            System.err.println("[Composer] WARNING: Single document processing runs always single threaded, worker threads are ignored!");
        }

        try {
            TypeSystemDescription desc = instantiate_pipeline(idPipeline);
            DUUIEither start = run_pipeline(jc,idPipeline);

            String cas = start.getAsString();
            System.out.printf("[Composer] Result %s\n", cas);
            jc = start.getAsJCas();

            System.out.printf("[Composer] Total number of transforms in pipeline %d\n", start.getTransformSteps());
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


    public static void main(String[] args) throws Exception {
        // Use two worker threads, at most 2 concurrent pipelines can run
        DUUIComposer composer = new DUUIComposer().withCompressionMethod("none");

        // Instantiate drivers with options
        DUUILocalDriver driver = new DUUILocalDriver()
                .withTimeout(10000);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
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
        /*composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.DUUILocalDriver.Component("kava-i.de:5000/secure/test_image")
    /*    composer.add(new DUUILocalDriver.Component("kava-i.de:5000/secure/test_image")
                        .withScale(2)
                        .withImageFetching()
                        .withRunningAfterDestroy(false)
                        .withRegistryAuth("SET_USERNAME_HERE","SET_PASSWORD_HERE")
                , org.texttechnologylab.DockerUnifiedUIMAInterface.DUUILocalDriver.class);*/

        // Remote driver handles all pure URL endpoints
        //composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIRemoteDriver.Component("http://127.0.0.1:9714")
        //                .withScale(2),
        //        org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIRemoteDriver.class);
        /*composer.add(new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
                        .withScale(2),
                DUUIRemoteDriver.class);*/

        // UIMA Driver handles all native UIMA Analysis Engine Descriptions
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class,
                        BreakIteratorSegmenter.PARAM_LANGUAGE,"en")
        ).withScale(2), DUUIUIMADriver.class);

        //composer.add(new org.texttechnologylab.DockerUnifiedUIMAInterface.DUUISwarmDriver.Component("localhost:5000/pushed")
        //                .withFromLocalImage("new:latest")
        //                .withScale(3)
        //                .withRunningAfterDestroy(false)
        //        , org.texttechnologylab.DockerUnifiedUIMAInterface.DUUISwarmDriver.class);

        //System.out.println("Generating full concurrency graph. WARNING: This needs a full pipeline instantiation.");

        // This takes a bit of time since the full pipeline is begin build and evaluatd.
        //composer.printConcurrencyGraph();



        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        // Run single document
        //composer.run(jc);

        // Run Collection Reader
        composer.run(createReaderDescription(TextReader.class,
                TextReader.PARAM_SOURCE_LOCATION, "test_corpora/**.txt",
                TextReader.PARAM_LANGUAGE, "en"));

  }
}
