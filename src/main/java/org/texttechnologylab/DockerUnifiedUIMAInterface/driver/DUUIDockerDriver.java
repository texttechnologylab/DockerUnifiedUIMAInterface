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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.javatuples.Pair;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.PipelineProgress;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceViews;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIRestClient;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;
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
    final AtomicBoolean _isInstanstiated = new AtomicBoolean(false);
    final DockerDriverView _stats = new DockerDriverView(); 
    Function<String, Boolean> _container_resumer = this::start; 
    Function<String, Boolean> _container_pauser = this::kill; 
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

        _active_components = new HashMap<>();
    }

    public DUUIDockerDriver withContainerPause() {
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
        int scale = comp.getScale() <= DUUIComposer.Config.strategy().getCorePoolSize() ? 
            comp.getScale() : DUUIComposer.Config.strategy().getCorePoolSize();
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

                // _wsclient = null;
                // if (comp.isWebsocket()) {
                //     String url = "ws://127.0.0.1:" + String.valueOf(port);
                //     _wsclient = new DUUIWebsocketAlt(
                //             url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, comp.getWebsocketElements());
                // }
                _layers.put(uuid, layer);
                comp.addInstance(new ComponentInstance(containerid, port, layer));
                _stats.addStartUp(uuidCopy, Duration.ofNanos(System.nanoTime() - startUp));
            }
            catch(Exception e) {
                _interface.stop_container(containerid);
                _stats.crashes.get(uuid).incrementAndGet();
                throw e;
            }
        }
        fillUpReserves(uuid);
        _isInstanstiated.set(true); 
        return uuid;
    }

    void fillUpReserves(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }

        final int totalInstances = comp._instancesSet.size() + comp._backup.size();
        final int fillUpRange = comp.getScale() - totalInstances;
        if (fillUpRange <= 0) return;
        int tries = 0;
        for (int i = 0; i < fillUpRange; i++) {
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
            comp.addInstance(new ComponentInstance(containerid, 0, layer));
        }
    }

    public boolean scaleUp(ComponentInstance instance) {
        boolean started = _container_resumer.apply(instance.getContainerId());
        try {
            instance._port = _interface.extract_port_mapping(instance.getContainerId());
            String url = "http://127.0.0.1:" + String.valueOf(instance._port);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(_container_timeout))
                .GET()
                .build();

            while (!ping(url, _client, request, 1)) {
            }
        } catch (Exception e) {
            started = false;
        }
        
        return started;
    }

    public boolean start(String containerId) {
        try {
            _interface.start_container(containerId);
            return true;
        } catch (Exception e) {
            // System.out.println("START CONTAINER FAIL: ");
            // System.out.printf("%s: %s", e, e.getLocalizedMessage());
            _interface.stop_container(containerId);
            return false;
        }
    }

    public boolean kill(String containerId) {
        try {
            _interface.kill_container(containerId);
            return true;
        } catch (Exception e) {
            // System.out.println("START CONTAINER FAIL: ");
            // System.out.printf("%s: %s", e, e.getLocalizedMessage());
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

    public void destroy(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }
        _isInstanstiated.set(false); 
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            int containerCount = comp._instancesSet.size();
            String image = comp.getImageName(); 
            comp._backup.drainTo(comp._instancesSet);
            for (ComponentInstance inst : comp._instancesSet) {
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
            
            // boolean downScalable = pipeline.isComponentCompleted(component.getUniqueComponentKey());
            final BooleanSupplier downScalable = () -> {
                final int idleInstances = component._instances.size();
                final int coreSize = Config.strategy().getCorePoolSize();
                return idleInstances > coreSize;// && pipeline.isCompleted(uuid);
            };
            final String uuid = component.getUniqueComponentKey();
            final int level = pipeline.getPipelineLevel(component.getUniqueComponentKey());
            final boolean isNext = pipeline.getNextLevel() == level;
            final boolean isCurrent = pipeline.getCurrentLevel() == level;
            // System.out.printf("======= COMPONENT  %s    LEVEL %d IsCurrent  %s   IsNext %s  ========\n", 
            //     component.getSignature(), level, isCurrent, isNext && pipeline.isBatchReadIn());

            // Preemptive Up-Scaling
            if (isNext && pipeline.isBatchReadIn()) {
                final double progress = pipeline.getLevelProgress();
                final long acceleration = pipeline.getAcceleration();
                final int backupsize = component.getScale() - Config.strategy().getCorePoolSize();
                final boolean shouldScale = _stats.shouldScale(acceleration, progress, uuid, backupsize, pipeline.getCurrentLevel());
                if (shouldScale) {
                    final long s = System.nanoTime();
                    final int levelSize = pipeline.getLevelSize(level, this.getClass());
                    final int poolMax = Config.strategy().getMaxPoolSize();
                    final int max = (int) Math.ceil(poolMax / (float) levelSize); 
                    if (component._instancesSet.size() < max) {
                        IntStream.rangeClosed(1, Integer.min(max, component._backup.size()))
                        .parallel()
                        .forEach(x -> component.scaleUp());
                    }
                    System.out.printf("SCALE UP REAL FILL UP TIME MS: %d \n", 
                        Duration.ofNanos(System.nanoTime() - s).toMillis());
                    _stats.addStartUp(uuid, Duration.ofNanos(System.nanoTime() - s));
                } else {
                    System.out.println("                                DOWN SCALING NOW");
                    while (downScalable.getAsBoolean()) {
                        component.scaleDown();
                    }
                }
            }// Matching replica size with thread-pool-sizes
            else if (isCurrent) {
                // final int poolSize = pipeline.getComponentPoolSize(uuid);
                // while (component.scaleUp() && component._instancesSet.size() < poolSize) {
                //     System.out.println("POOL SIZE MATCHING");
                // }
            } else if (!isCurrent && !isNext) {
                
                while (downScalable.getAsBoolean()) {
                    component.scaleDown();
                }
            }

            // fillUpReserves(component._uniqueComponentKey);
        }
    }

    @Override
    public DockerDriverView collect() {
        _stats.update();
        return _stats;
    }
    
    public int totalContainers() {
        return _active_components.entrySet().stream()
            .map(Entry::getValue)
            .map(InstantiatedComponent.class::cast)
            .mapToInt(c -> c._instancesSet.size())
            .sum();
    }

    private class DockerDriverView implements ResourceView {

        final Map<String, DockerContainerView> _views;
        final Collection<DockerContainerView> _viewsSet;
        
        Map<String, AtomicInteger> crashes = new ConcurrentHashMap<>(20);
        Map<String, Duration> _avgStartUp = new ConcurrentHashMap<>();

        DockerDriverView() {
            _views = new HashMap<>();
            _viewsSet = _views.values();
        }

        void init() {
            _active_components.values().stream()
            .map(comp -> (InstantiatedComponent) comp)
            .flatMap(comp -> comp.getContainers().stream())
            .filter(c -> !_views.containsKey(c))
            .map(container -> 
                new DockerContainerView(container.getValue0(), container.getValue1()))
            .forEach(cv -> _views.put(cv.container_id, cv));
        }

        public void update() {
            init();
            _viewsSet.forEach(cv -> cv.stats(_interface));
        }

        boolean scaleUp = false;            
        final long extraPunishmentMillis = 2000;
        final AtomicInteger prevLevel = new AtomicInteger(-1);
        Map<Integer, Long> punishments = new HashMap<>(10);


        boolean shouldScale(long acceleration, double progress, String nextComponent, int size, int currLevel) {
            // Reset to initial values
            if (acceleration <= 0)
                return false;

            final Duration fillUpTime = _avgStartUp.get(nextComponent);
            final long punishment = punishments.getOrDefault(prevLevel.get(), 0L) + extraPunishmentMillis;
            // final long fillUpTimeMs = fillUpTime.toMillis() * size + punishment;
            final long fillUpTimeMs = fillUpTime.toMillis() + extraPunishmentMillis;// + punishment;
            final long durationMS = TimeUnit.NANOSECONDS.toMillis(acceleration);
            final double fillUpPercentage = (((double) fillUpTimeMs) / ((double) durationMS)) % 100;
            scaleUp = (100.f - fillUpPercentage) <= progress*100.f;

            System.out.printf("PREEMPTIVE SCALING                                              %s \n", scaleUp);
            System.out.printf("PIPELINE PROGRESS:                                              %.2f \n", (progress * 100.f));
            System.out.printf("FILL_UP_PERCENTAGE = FILL_UP_TIME_MS / EST_PERCENTAGE_TIME ---= %.2f \n", (100.f - fillUpPercentage) % 100);
            System.out.printf("%.2f             = %d          / %d \n", fillUpPercentage, fillUpTimeMs, durationMS);
            return scaleUp;
           
        }

        void addPunishment(long punishmentMillis) {
            punishments.merge(prevLevel.get(), punishmentMillis, Long::sum);

        }

        void addStartUp(final String uuid, final Duration startUp) {
            final BiFunction<Duration, Duration, Duration> average = (now, curr) -> {
                return Duration.ofMillis(Long.max(now.toMillis(), curr.toMillis()));

                // final long avg = (long) (Long.sum(now.toMillis(), curr.toMillis()) / (double) 2.f); 
                // return Duration.ofNanos(avg);
                // final long sum = now.toNanos() + curr.toNanos();
                // final long avg = (long) (((double) sum)/ 2f);
            };
            _stats._avgStartUp.merge(uuid, startUp, average);
        } 
    }

    static class ComponentInstance implements IDUUIUrlAccessible {
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

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        BlockingQueue<ComponentInstance> _instances;
        BlockingQueue<ComponentInstance> _backup;
        Set<ComponentInstance> _instancesSet; 
        DUUIPipelineComponent _component;
        String _uniqueComponentKey; 
        Signature _signature; 
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
            _instancesSet = ConcurrentHashMap.newKeySet();
        }

        public void tryScaleUp() throws InterruptedException {
            _scaleUpLock.lock();
            try {
                int totalScale = _driver.totalContainers();
                if ((_instancesSet.size() < getScale() 
                    && totalScale < Config.strategy().getMaxPoolSize())
                    || _instancesSet.size() < Config.strategy().getCorePoolSize()) { 
                    boolean scaledUp = scaleUp();
                    if (!scaledUp) {
                        System.out.printf("[%s][%s] SCALING UP FAILED %s\n", 
                            Thread.currentThread().getName(), 
                            this.getClass().getSimpleName(),
                            getImageName());
                    }
                }
            } finally {
                _scaleUpLock.unlock();
            }
        }

        public boolean scaleUp() {
            // _scaleUpLock.lock();
            // try {
            final ComponentInstance comp = _backup.poll();
            if (comp == null) return false; 
            final boolean success = _driver.scaleUp(comp);
            if (!success) return false;
            addInstance(comp);
            return true;
            // } finally {
            //     _scaleUpLock.unlock();
            // }
        }

        public void scaleDown() {
            // _scaleUpLock.lock();
            // try {
            ComponentInstance _instance = _instances.poll();
            if (_instance == null) return;
            _instancesSet.remove(_instance);
            final long startUp = System.nanoTime();
            _driver._container_pauser.apply(_instance.getContainerId());
            final Duration pun = Duration.ofNanos(System.nanoTime() - startUp);
            _driver._stats.addPunishment(pun.toMillis());
            _driver._stats.addStartUp(_uniqueComponentKey, pun);
            _backup.add(_instance);
            // } finally {
            //     _scaleUpLock.unlock();
            // }
        }

        public void addInstance(ComponentInstance inst) {
            if (_driver.aliveAndRunning(inst.getContainerId())) {
                if (_instances.offer(inst)) {
                    _instancesSet.add(inst);
                }
            } else {
                _backup.add(inst);
            }

        }
        
        public void returnInstance(IDUUIUrlAccessible access) throws InterruptedException {
            if (_driver.aliveAndRunning(((ComponentInstance)access).getContainerId())) {
                _instances.offer((ComponentInstance) access); // TODO: Only return uncrashed containers
                return;
            }

            cleanCrashedContainer((ComponentInstance) access);

            // tryScaleUp();
            long start = System.nanoTime();
            scaleUp();
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
                    }
                } 
                // tryScaleUp();
                long start = System.nanoTime();
                scaleUp();
                url = getInstances().take();
                DUUIComposer.totalscalingwait.addAndGet(System.nanoTime() - start);
                if (!_driver.aliveAndRunning(url.getContainerId())) {
                    cleanCrashedContainer(url);
                    throw new InterruptedException(
                        format("[%s][%s] Component instances failed too many times: %s", 
                            Thread.currentThread().getName(), 
                            this.getClass().getSimpleName(),
                            getImageName())
                    );
                }
                return url;
            } catch (Exception e) {
                throw new InterruptedException(
                    format("[%s][%s] Polling instances failed. ", 
                        Thread.currentThread().getName(), 
                        this.getClass().getSimpleName(), e)
                );
            }
        }
         
        public ArrayList<Pair<String, String>> getContainers() {
            // TODO: Avoid creating array in this call
            ArrayList<Pair<String, String>> res = new ArrayList<>(_instancesSet.size());
            for (ComponentInstance instance : _instancesSet) {
                res.add(Pair.with(instance._container_id, 
                    _component.getDockerImageName(""))
                );
            }

            return res; 
        }

        void cleanCrashedContainer(ComponentInstance instance) {
            _instancesSet.remove(instance);
            System.out.printf("[%s][%s] CONTAINER CRASHED %s", 
                Thread.currentThread().getName(), 
                this.getClass().getSimpleName(),
                getImageName());
            _driver._stats.crashes.get(_uniqueComponentKey).incrementAndGet();
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
