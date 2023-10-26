package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import static java.lang.String.format;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIRestClient;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.PipelineProgress;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;
import org.xml.sax.SAXException;

public class DUUIDockerDriver implements IDUUIConnectedDriver, IDUUIResource {
    transient DUUIDockerInterface _interface;
    transient HttpClient _client;
    IDUUIConnectionHandler _wsclient;

    Map<String, IDUUIInstantiatedPipelineComponent> _active_components;
    Map<String, IDUUICommunicationLayer> _layers;
    int _container_timeout;
    DUUILuaContext _luaContext;
    
    ArrayList<JSONObject> _resource_container = null; 
    final DockerDriverView _stats; 
    Function<String, Boolean> _container_resumer = this::start; 
    Function<String, Boolean> _container_pauser = this::kill; 
    boolean _withPause = false;
    final JCas _basic; 


    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUIDockerDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = DUUIRestClient._client;
        // _client = HttpClient.newBuilder().build();

        _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("de");
        _basic.setDocumentText("Hallo, Welt!");
        _container_timeout = 10000;

        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _active_components = new ConcurrentHashMap<>();
        _layers = new ConcurrentHashMap<>();
        _luaContext = null;
        _stats = new DockerDriverView(_active_components, _interface, _withPause);
    }

    public DUUIDockerDriver(int timeout) throws IOException, UIMAException, SAXException {
        _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("de");
        _basic.setDocumentText("Hallo, Welt!");

        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder()
            .executor(Runnable::run)    
            .connectTimeout(Duration.ofSeconds(timeout)).build();

        _container_timeout = timeout;

        _active_components = new ConcurrentHashMap<>();

        _stats = new DockerDriverView(_active_components, _interface, _withPause);
    }

    public DUUIDockerDriver withContainerPause() {
        _withPause = true;
        _container_pauser = this::pause;
        _container_resumer = this::resume;
        return this; 
    }

    public Map<String, IDUUIInstantiatedPipelineComponent> getComponents() {
        return _active_components;
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    public DUUIDockerDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }
    
    public boolean canAccept(DUUIPipelineComponent comp) {
        return comp.getDockerImageName()!=null;
    }

    public void printConcurrencyGraph(String uuid) {
        IDUUIInstantiatedPipelineComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerLocalDriver][%s]: Maximum concurrency %d\n",uuid,component.getInstances().size());
    }

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_components.contains(uuid.toString())) { // Global checking of uuid uniqueness
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component, uuid, this);

        if (!comp.getImageFetching()) {
            if(comp.getUsername() != null) {
                System.out.printf("[DockerLocalDriver] Attempting image %s download from secure remote registry\n",comp.getImageName());
            }
            _interface.pullImage(comp.getImageName(),comp.getUsername(),comp.getPassword());
            System.out.printf("[DockerLocalDriver] Pulled image with id %s\n",comp.getImageName());
        }
        else {
            if(!_interface.hasLocalImage(comp.getImageName())) {
                throw new InvalidParameterException(format("Could not find local docker image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?",comp.getImageName()));
            }
        }
        System.out.printf("[DockerLocalDriver] Assigned new pipeline component unique id %s\n", uuid);
        String digest = _interface.getDigestFromImage(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(),digest);
        System.out.printf("[DockerLocalDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(),comp.getPipelineComponent().getDockerImageName());
        
        _active_components.put(uuid, comp);
        _stats.crashes.put(uuid, new AtomicInteger(0));        
        comp.setScale(DUUIComposer.Config.strategy().getMaxPoolSize()); // If scale fixed, nothing changes
        int scale = comp.getScale();
        for (int i = 0; i < scale; i++) {
            final long startUp = System.nanoTime();

            String containerid = _interface.run(comp.getPipelineComponent().getDockerImageName(), comp.usesGPU(), false, 9714,false);
            int port = _interface.extract_port_mapping(containerid);
            try {
                if (port == 0) {
                    throw new UnknownError("Could not read the container port!");
                }
                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = get_communication_layer("http://127.0.0.1:" + String.valueOf(port), jc, _container_timeout, _client,(msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, scale, msg);
                },_luaContext, skipVerification || Config.isParallel()); // TODO: Make alternative verification when Inputs and Outputs are necessary
                System.out.printf(
                    "[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", 
                    uuid, i + 1, comp.getScale(), comp.getImageName(), port);
                    
                _layers.put(uuid, layer);
                comp.addInstance(new ComponentInstance(containerid, port, layer));
                _stats.addStartUp(uuidCopy, Duration.ofNanos(System.nanoTime() - startUp));
            }
            catch(Exception e) {
                _interface.stop_container(containerid);
                throw e;
            }
        }
        fillUpReserves(uuid);
        return uuid;
    }

    void fillUpReserves(String uuid) {
        final InstantiatedComponent comp = (InstantiatedComponent) _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }

        int tries = 0;
        final int scale = comp.getScale();
        while (scale > comp.getTotalSize()) {
            String containerid = null;
            boolean created = false;
            do {
                try {
                    containerid = _interface.create(comp.getPipelineComponent().getDockerImageName(), comp.usesGPU(),false, 9714,false);
                    created = true;
                } catch (Exception e) {}    
            } while (tries++<3 && !created);

            if (containerid == null) continue;

            final IDUUICommunicationLayer layer = _layers.get(uuid).copy();
            final ComponentInstance instance = new ComponentInstance(containerid, 0, layer);
            if (_withPause) {
                scaleUp(instance, true);
                _container_pauser.apply(containerid);
            }
            comp.addInstance(instance);
        }
    }

    boolean scaleUp(ComponentInstance instance) {
        return scaleUp(instance, !_withPause);
    }

    boolean scaleUp(ComponentInstance instance, boolean verify) {
        boolean started = _container_resumer.apply(instance.getContainerId());
        if (verify && started) {
            try {
                instance._port = _interface.extract_port_mapping(instance.getContainerId());
                String url = "http://127.0.0.1:" + String.valueOf(instance._port);
                final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(_container_timeout))
                    .GET()
                    .build();
    
                while (!ping(url, _client, request, 4)) {
                }
            } catch (Exception e) {
                System.out.println("[DockerLocalDriver] Error on ping: " + e.getMessage());
                started = false;
            }
        }

        if (!started) {
            _interface.stop_container(instance.getContainerId());
        }

        return started;
    }

    public boolean start(String containerId) {
        try {
            _interface.start_container(containerId);
            return true;
        } catch (Exception e) {
            System.out.println("[DockerLocalDriver] Error on start: " + e.getMessage());
            _interface.stop_container(containerId);
            return false;
        }
    }

    public boolean kill(String containerId) {
        try {
            _interface.kill_container(containerId);
            return true;
        } catch (Exception e) {
            System.out.println("[DockerLocalDriver] Error on kill: " + e.getMessage());
            _interface.stop_container(containerId);
            return false;
        }
    }

    public boolean resume(String containerId) {
        try {
            _interface.unpause_container(containerId);
            return true;
        } catch (Exception e) {
            _interface.stop_container(containerId);
            return false;
        }
    }
    
    public boolean pause(String containerId) {
        try {
            _interface.pause_container(containerId);
            return true;
        } catch (Exception e) {
            _interface.stop_container(containerId);
            return false;
        }
    }

    boolean aliveAndRunning(String containerId) {
        return isAlive(containerId) && _interface.is_container_running(containerId);
    }

    public boolean isAlive(String containerId) {
        return _interface.is_container_alive(containerId);
    }
 
    public void shutdown() {
        try {
            _interface.getDockerClient().close();
        } catch (IOException e) {
        }
        try {
            _interface = new DUUIDockerInterface();
        } catch (IOException e) {
        }
        _stats.reset();
    }

    public void destroy(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _active_components.remove(uuid);
        if (comp == null) {
            System.out.println("Invalid UUID, this component has not been instantiated by the local driver.");
            return;
            // throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }

        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            int containerCount = comp._instancesSet.size();
            String image = comp.getImageName(); 
            List<ComponentInstance> all = new ArrayList<>(comp._instancesSet.values());
            comp._backup.drainTo(all);
            for (ComponentInstance inst : all) {
                // final boolean active = _container_resumer.apply(inst.getContainerId());
                System.out.printf("[DockerLocalDriver][Replication %d/%d] Stopping docker container %s...\n",
                    counter, containerCount, image
                );
                _interface.stop_container(inst.getContainerId());
                counter+=1;
            }
        }
        
    }

    @Override
    public void scale(ResourceViews statistics) {
        final PipelineProgress pipeline = statistics.getComponentProgress();
        if (pipeline == null) return;

        for (IDUUIInstantiatedPipelineComponent icomponent : _active_components.values()) {

            final InstantiatedComponent component = (InstantiatedComponent) icomponent;            
            final String uuid = component.getUniqueComponentKey();
            final int level = pipeline.getPipelineLevel(uuid);
            final int currLevelSize = pipeline.getLevelSize(pipeline.getCurrentLevel());
            final int levelSize = pipeline.getLevelSize(level);
            final int poolMax = component.getScale();
            final int optimal = poolMax;// Math.round(poolMax / (float) levelSize);
            final boolean isReady = pipeline.isBatchReadIn();
            

            final BooleanSupplier downScalable = () -> {
                final int idleInstances = component._instances.size();
                final int coreSize = Config.strategy().getCorePoolSize();
                final boolean beforeEnd = pipeline.hasShutdown();
                final boolean complete = pipeline.getComponentProgress(uuid) == 0.0 || pipeline.isCompleted(uuid);
                final boolean notNeededYet = level > 2 || pipeline.isBatchReadIn();
                return idleInstances > coreSize && complete && !beforeEnd && notNeededYet;
            };

            final boolean upscalable = component._backup.size() > 0;
            final boolean isNext = pipeline.getNextLevel() == level;
            final boolean isInProgress = pipeline.getComponentProgress(uuid) > 0.0 && !pipeline.isCompleted(uuid);
            final boolean isCurrent = pipeline.getCurrentLevel() == level || isInProgress;
            final boolean isScalable = component.getScale() > Config.strategy().getCorePoolSize();

            if (component.getTotalSize() < poolMax) {
                fillUpReserves(component._uniqueComponentKey);
            }

            if (isCurrent && isNext) {
                continue;
            } else if (isCurrent) { // Matching replica size with thread-pool-sizes
                component.preemptiveScaleUp(optimal);
            } else if (!isScalable) {
                continue;
            } else if (isNext && isReady && pipeline.getLevelProgress() > 0.1) { // Preemptive Up-Scaling
                // final double progress = pipeline.getLevelProgress();
                // final long remainingNanos = pipeline.getRemainingNanos();
                // final int backupsize = component._backup.size();
                // final boolean shouldScale = _stats.shouldScale(remainingNanos, progress, uuid, backupsize, pipeline.getCurrentLevel(), levelSize, currLevelSize);
                // if ((shouldScale && progress >= 0.1) && upscalable) { // Avoid entering if-Block if already preemptively scaled.
                    final long s = System.nanoTime();
                    component.preemptiveScaleUp(optimal);
                    // System.out.printf("SCALE UP REAL FILL UP TIME MS: %d %s\n", 
                    //     Duration.ofNanos(System.nanoTime() - s).toMillis(), component.getSignature());
                    _stats.addStartUp(uuid, Duration.ofNanos(System.nanoTime() - s));
                // }
            } else if (!isCurrent && !isNext) { // Down-Scaling
                if (downScalable.getAsBoolean()) {
                    component.fullScaleDown();
                }
            }
        }
    }

    @Override
    public DockerDriverView collect() {
        // _stats.update();
        return _stats;
    }

    public static class DockerDriverView implements ResourceView {

        final Map<String, DockerContainerView> _views;
        final Collection<DockerContainerView> _viewsSet;
        final Collection<IDUUIInstantiatedPipelineComponent> _comps;
        final DUUIDockerInterface _interface;
        final boolean _pause;
        final BiFunction<Duration, Duration, Duration> max = (now, curr) -> {
            final long currNanos = Math.max(curr.toNanos(), 2000*1_000_000); // at least 2s wait time
            final long max = (long) Math.max(now.toNanos(), currNanos); 
            return Duration.ofNanos(max);
        };
        
        Map<String, AtomicInteger> crashes = new ConcurrentHashMap<>(20);

        DockerDriverView(Map<String, IDUUIInstantiatedPipelineComponent> comps, DUUIDockerInterface iinterface, boolean pause) {
            _views = new HashMap<>();
            _viewsSet = _views.values();
            _comps = comps.values();
            _interface = iinterface;
            _pause = pause;
        }
            
        final AtomicInteger prevLevel = new AtomicInteger(-1);
        double prevProgress = 0.0;
        boolean prevScaleUp = false;
        final Map<Integer, Long> punishments = new HashMap<>();
        final Map<String, Duration> avgStartUp = new ConcurrentHashMap<>();
        boolean shouldScale(long remainingNanos, double progress, String nextComponent, int size, int currLevel, int levelSize, int currLevelSize) {
            if (prevProgress == progress) return prevScaleUp; // Reduce number of iterations
            prevLevel.set(currLevel);
            prevProgress = progress;

            // Calculating time to start up back up replicas of next component
            final long extraPunishmentMillis = _pause ? 0L : 5000L;
            final long punishment = punishments.getOrDefault(prevLevel.get(), 0L) + extraPunishmentMillis;
            final long fillUpTimeMs = avgStartUp.get(nextComponent).toMillis() + punishment;
            
            // Remaining time in pipeline for current component
            final long remainingDurationMS = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            
            final int levelCorrection = Math.min(levelSize / currLevelSize, 1);
            final int poolCorrection = Math.max((Config.strategy().getMaxPoolSize() / 5) * 4000,  2000);
            final boolean scaleUp = remainingDurationMS <= (fillUpTimeMs + poolCorrection) * levelCorrection; 
            return (prevScaleUp = scaleUp);
           
        }

        void addPunishment(long punishmentMillis) {
            punishments.merge(prevLevel.get(), punishmentMillis, Long::sum);
        }

        void addStartUp(final String uuid, final Duration startUp) {
            avgStartUp.merge(uuid, startUp, max);
        } 

        void init() {
            _comps.stream()
            .map(comp -> (InstantiatedComponent) comp)
            .forEach(comp -> {
                comp.getContainers().stream()
                    .filter(Predicate.not(_views::containsKey))
                    .map(c -> new DockerContainerView(c, comp.getImageName()))
                    .forEach(v -> _views.put(v.getContainerId(), v));
            });
        }

        public Collection<DockerContainerView> getContainerViews() {
            return _viewsSet;
        }

        public void update() {
            init();
            _viewsSet.forEach(cv -> cv.stats(_interface));
        }

        public boolean contains(String containerId) {
            return _views.containsKey(containerId);
        }

        void reset() {
            _views.clear();

            prevLevel.set(-1);       
            prevProgress = 0.0;
            prevScaleUp = false;
            punishments.clear();
            avgStartUp.clear();
        }
    }

    class ComponentInstance implements IDUUIUrlAccessible {
        private String _container_id;
        private int _port;
        private IDUUIConnectionHandler _handler;
        private IDUUICommunicationLayer _communicationLayer;

        public ComponentInstance(String id, int port, IDUUICommunicationLayer communicationLayer) {
            _container_id = id;
            _port = port;
            _communicationLayer = communicationLayer;
        }

        public ComponentInstance(String id, int port, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _container_id = id;
            _port = port;
            _communicationLayer = layer;
            _handler = handler;
        }
        
        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        String getContainerId() {
            return _container_id;
        }

        int getContainerPort() {
            return _port;
        }

        public String generateURL() {
            return format("http://127.0.0.1:%d", _port);
        }

        String getContainerUrl() {
            return format("http://127.0.0.1:%d", _port);
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        LinkedBlockingQueue<ComponentInstance> _instances;
        BlockingQueue<ComponentInstance> _backup;
        ConcurrentHashMap<String, ComponentInstance> _instancesSet; 
        DUUIPipelineComponent _component;
        String _uniqueComponentKey; 
        Signature _signature;
        String image; 
        final DUUIDockerDriver _driver;   
        final ReentrantLock _scaleUpLock = new ReentrantLock(true);     

        InstantiatedComponent(DUUIPipelineComponent comp, String uuid, DUUIDockerDriver driver) {
            _driver = driver;
            _component = comp;
            if (comp.getDockerImageName() == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _uniqueComponentKey = uuid;

            _instances = new LinkedBlockingQueue<ComponentInstance>();
            _backup = new LinkedBlockingQueue<ComponentInstance>();
            _instancesSet = new ConcurrentHashMap<>();
        }

        int getUsedSize() {
            // _scaleUpLock.lock();
            try {
                return _instancesSet.size() - _backup.size();
            } finally {
                // _scaleUpLock.unlock();
            }
        }

        int getTotalSize() {
            // _scaleUpLock.lock();
            try {
                return _instancesSet.size();
            } finally {
                // _scaleUpLock.unlock();
            }
        }

        public void preemptiveScaleUp(int max) {
            _scaleUpLock.lock();
            try {
                if (_backup.size() < 1) return;
                final ArrayList<ComponentInstance> drain = new ArrayList<>(_backup.size());
                _backup.drainTo(drain);
                drain.parallelStream()
                    .forEach(comp -> {
                        if (_driver.scaleUp(comp)) {
                            _instances.add(comp);
                        }
                        else _instancesSet.remove(comp.getContainerId(), comp);
                    });

                    System.out.printf("PRE-- %s: Backup: %d| Instance: %d| Set: %d\n",
                         getSignature(), _backup.size(), _instances.size(), _instancesSet.size());
            } finally {
                _scaleUpLock.unlock();
            }
        }

        public boolean scaleUp() {
            _scaleUpLock.lock();
            try {
                final long startUp = System.nanoTime();
                final ComponentInstance comp = _backup.poll();
                final boolean success = comp == null ? false : _driver.scaleUp(comp);

                if (!success) {
                    if (comp != null) 
                        _instancesSet.remove(comp.getContainerId(), comp);
                    return false;
                } 
                _instances.add(comp);
                final Duration pun = Duration.ofNanos(System.nanoTime() - startUp);
                _driver._stats.addPunishment(pun.toMillis());
                return true;
            } finally {
                _scaleUpLock.unlock();
            }
        }

        public void scaleDown() {
            _scaleUpLock.lock();
            try {
                ComponentInstance _instance = _instances.poll();
                if (_instance == null) return;
                _driver._container_pauser.apply(_instance.getContainerId());
                _backup.add(_instance);
            } finally {
                _scaleUpLock.unlock();
            }
        }

        void fullScaleDown() {
            _scaleUpLock.lock();
            try {
                final ArrayList<ComponentInstance> drain = new ArrayList<>(_instances.size());
                final int max = Math.max(getScale() - Config.strategy().getCorePoolSize(), 0);
                _instances.drainTo(drain, max);
                drain.parallelStream()
                .forEach(comp -> {
                    _driver._container_pauser.apply(comp.getContainerId());
                    _backup.add(comp);
                });
            } finally {
                _scaleUpLock.unlock();
            }
        }
        
        void cleanCrashedContainer(ComponentInstance instance) {
            _scaleUpLock.lock();
            try {
                _driver._interface.stop_container(instance._container_id);
                _instancesSet.remove(instance.getContainerId());
                System.out.printf("[%s][%s] CONTAINER CRASHED %s", 
                    Thread.currentThread().getName(), 
                    this.getClass().getSimpleName(),
                    getImageName());
                _driver._stats.crashes.get(_uniqueComponentKey).incrementAndGet();
            } finally {
                _scaleUpLock.unlock();
            }
        }

        public void addInstance(ComponentInstance inst) {
            _scaleUpLock.lock();
            try {
                if (_driver.aliveAndRunning(inst.getContainerId())) {
                    _instances.add(inst);
                } else _backup.add(inst);

                _instancesSet.put(inst.getContainerId(), inst);
            } finally {
                _scaleUpLock.unlock();
            }

        }
        
        public void returnInstance(IDUUIUrlAccessible access) throws InterruptedException {
            if (_driver.aliveAndRunning(((ComponentInstance)access).getContainerId())) {
                _instances.add((ComponentInstance) access); // TODO: Only return uncrashed containers
                return;
            }

            cleanCrashedContainer((ComponentInstance) access);
        
            long start = System.nanoTime();
            if (_instances.isEmpty()) scaleUp();
            DUUIComposer.totalscalingwait.addAndGet(System.nanoTime() - start);
        }

        @Override
        public IDUUIUrlAccessible takeInstance() throws InterruptedException {
            try {
                ComponentInstance url = getInstances().poll();

                if (url != null) {
                    if (_driver.aliveAndRunning(url.getContainerId())) // if false, container likely crashed or worse
                        return url; 
                    else {
                        cleanCrashedContainer(url);
                        long start = System.nanoTime();
                        scaleUp();
                        DUUIComposer.totalscalingwait.addAndGet(System.nanoTime() - start);
                    }
                } 

                if (_instancesSet.size() == 0) {
                    long start = System.nanoTime();
                    _driver.fillUpReserves(_uniqueComponentKey);
                    scaleUp();
                    DUUIComposer.totalscalingwait.addAndGet(System.nanoTime() - start);
                }
                final long startUp = System.nanoTime();
                url = getInstances().take();
                final Duration pun = Duration.ofNanos(System.nanoTime() - startUp);
                _driver._stats.addPunishment(pun.toMillis());
                if (!_driver.aliveAndRunning(url.getContainerId())) {
                    cleanCrashedContainer(url);
                    throw new RuntimeException(
                        format("[%s][%s] Component instances failed too many times: %s", 
                            Thread.currentThread().getName(), 
                            this.getClass().getSimpleName(),
                            getImageName())
                    );
                }
                return url;
            } catch (Exception e) {
                throw new RuntimeException(
                    format("[%s][%s] Polling instances failed. ", 
                        Thread.currentThread().getName(), 
                        this.getClass().getSimpleName(), e)
                );
            }
        }
         
        public List<String> getContainers() {
            return _instancesSet.values().stream()
                .map(ComponentInstance::getContainerId)
                .filter(_driver::aliveAndRunning)
                .collect(Collectors.toList()); 
        }
        
        public BlockingQueue<ComponentInstance> getInstances() {
            return _instances;
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        public String getPassword() {
            return getPipelineComponent().getDockerAuthPassword();
        }

        public String getUsername() {
            return getPipelineComponent().getDockerAuthUsername();
        }

        public boolean getImageFetching() {
            return getPipelineComponent().getDockerImageFetching(false);
        }

        public String getImageName() {
            return getPipelineComponent().getDockerImageName();
        }

        public boolean getRunningAfterExit() {
            return getPipelineComponent().getDockerRunAfterExit(false);
        }

        public boolean usesGPU() {
            return getPipelineComponent().getDockerGPU(false);
        }

        @Override
        public void setSignature(Signature sig) {
            _signature = sig;  
        }

        @Override
        public Signature getSignature() {
            return _signature; 
        }
    }

    public static class Component implements IDUUIDriverComponent<Component> {
        private DUUIPipelineComponent _component;

        public Component(String target) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
        }

        public Component getComponent() {
            return this;
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public Component withRegistryAuth(String username, String password) {
            _component.withDockerAuth(username,password);
            return this;
        }

        public Component withImageFetching() {
            _component.withDockerImageFetching(true);
            return this;
        }

        public Component withGPU(boolean gpu) {
            _component.withDockerGPU(gpu);
            return this;
        }

        public Component withRunningAfterDestroy(boolean run) {
            _component.withDockerRunAfterExit(run);
            return this;
        }

        public Component withWebsocket(boolean b) {
            _component.withWebsocket(b);
            return this;
        }

        public Component withWebsocket(boolean b, int elements) {
            _component.withWebsocket(b, elements);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIDockerDriver.class);
            return _component;
        }
    }
}
