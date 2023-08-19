package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import static java.lang.String.format;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.javatuples.Pair;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ComponentProgress;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager.ResourceStatistics;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIRestClient;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;
import org.xml.sax.SAXException;

public class DUUIDockerDriver implements IDUUIConnectedDriver, IDUUIResource {
    transient DUUIDockerInterface _interface;
    transient HttpClient _client;
    IDUUIConnectionHandler _wsclient;

    Map<String, IDUUIInstantiatedPipelineComponent> _active_components;
    int _container_timeout;
    DUUILuaContext _luaContext;
    
    ArrayList<JSONObject> _resource_container = null; 
    final AtomicBoolean _isInstanstiated = new AtomicBoolean(false);
    final DockerStats _stats = new DockerStats(); 


    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUIDockerDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = DUUIRestClient._client;
        // _client = HttpClient.newBuilder().build();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _container_timeout = 10000;

        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _active_components = new ConcurrentHashMap<>();
        _luaContext = null;
    }

    public DUUIDockerDriver(int timeout) throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder()
            .executor(Runnable::run)    
            .connectTimeout(Duration.ofSeconds(timeout)).build();

        _container_timeout = timeout;

        _active_components = new HashMap<>();
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
        int scale = comp.getScale();

        for (int i = 0; i < scale; i++) {
            String containerid = _interface.run(comp.getPipelineComponent().getDockerImageName(), comp.usesGPU(), true, 9714,false);
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
                comp.addInstance(new ComponentInstance(containerid, port, layer));
            }
            catch(Exception e) {
                _interface.stop_container(containerid);
                _stats.crashes.get(uuid).incrementAndGet();
                throw e;
            }
        }
        comp.setScale(DUUIComposer.Config.strategy().getMaxPoolSize()); // If scale fixed, nothing changes
        _isInstanstiated.set(true); 
        return uuid;
    }

    public boolean scaleUp(String uuid, int repeat) {
        InstantiatedComponent comp = (InstantiatedComponent) _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }
        
        int instanceCount = comp._instancesSet.size(); 
        int tries = 0;
        boolean created = false;

        do {
            String containerid = null;
            try {
                containerid = _interface.run(
                    comp.getPipelineComponent().getDockerImageName(), 
                    false,
                    true, 
                    9714,
                    false
                );
                int port = _interface.extract_port_mapping(containerid);

                IDUUICommunicationLayer layer = get_communication_layer("http://127.0.0.1:" + String.valueOf(port), null, 3000, _client,(msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuid, instanceCount + 1, instanceCount + 1, msg);
                },_luaContext, true);
                System.out.printf(
                    "[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", 
                    uuid, instanceCount + 1, comp.getScale(), comp.getImageName(), port
                );

                created = true; 
                comp.addInstance(new ComponentInstance(containerid, port, layer));
            } catch (Exception e) {
                if (containerid != null) 
                    _interface.stop_container(containerid);
                System.out.printf("[DockerLocalDriver][%s] Scaling up component failed.%n", uuid);
                e.printStackTrace();
            }
        } while (tries++<=repeat && !created);

        if (!created) {
            _stats.crashes.get(uuid).incrementAndGet();
        }

        return created;
    }

    public boolean resume(String containerId) {
        try {
            _interface.unpause_container(containerId);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    public boolean pause(String containerId) {
        try {
            _interface.pause_container(containerId);
            return true;
        } catch (Exception e) {
            return false;
        }
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
    public void scale(ResourceStatistics statistics) {
        
        ComponentProgress pipeline = statistics.getComponentProgress();
        final int poolSize = Config.strategy().getMaxPoolSize();
        if (!_stats.initialScaling.get()) {
            Map<String, Integer> levels = pipeline.getComponentLevels();

            if (! levels.isEmpty()) {
                // Map<Integer, Integer> componentsPerLevel = new HashMap<>();
                // levels.forEach((component, level) -> 
                // {
                //     int prev = componentsPerLevel.getOrDefault(level, 0);
                //     prev++;
                //     if (_active_components.containsKey(component))
                //         componentsPerLevel.put(level, prev);
                // });

                levels.forEach((uuid, level) -> 
                {
                    InstantiatedComponent component = 
                        (InstantiatedComponent) _active_components.get(uuid);

                    // float currScale = (float) poolSize;
                    // float levelSize = (float) componentsPerLevel.get(level);
                    // int newScale = (int) Math.ceil(currScale/levelSize);
                    // component.setScale(newScale);
                    component.setScale(poolSize);
                });
                _stats.initialScaling.set(true);
            }
        }

        _stats.crashes.forEach((component, crashes) -> 
        {
            boolean downScalable = pipeline.isComponentCompleted(component);
            
            InstantiatedComponent obj = 
                (InstantiatedComponent) _active_components.get(component);

            int currentLevel = pipeline.getCurrentPipelineLevel();
            int componentLevel = pipeline.getComponentPipelineLevel(component);

            // if (crashes.get() >= 1) {
            //     System.out.printf("[DUUIResourceManager][DockerLocalDriver][%s] Scaling down!%n", 
            //         obj._signature);
            //     System.out.printf(
            //         "[DUUIResourceManager][DockerLocalDriver][%s] NUMBER OF CRASHES: %d %n", 
            //         obj._signature, crashes.get());
                
            //     obj.removeInstance();
            //     obj.setScale(obj._instancesSet.size());
            //     crashes.set(0);
            // }

            while ((obj._instancesSet.size() > obj.getScale()
                || obj._instances.size() > Config.strategy().getCorePoolSize())
                && currentLevel != componentLevel) {
                System.out.printf("[DUUIResourceManager][DockerLocalDriver][%s] Scaling down!%n", 
                                    obj._signature);
                obj.removeInstance();
            }
        });
    }

    @Override
    public Map<String, Object> collect() {

        if (! _isInstanstiated.get())
            return null;

        List<Map<String, Object>> stats = _active_components.values().stream()
            .map(comp -> (InstantiatedComponent) comp)
            .flatMap(comp -> comp.getContainers().stream())
            .map(container -> 
                getContainerStats(container.getValue0(), container.getValue1()))
            .collect(Collectors.toList());

        _stats.dispatchMap.put(this.getClass().getSimpleName(), stats);

        return _stats.dispatchMap;
    }

    Map<String, Object> getContainerStats(String containerId, String image) {
        
        Map<String, Object> containerStats; 
        boolean containerRegistered = _stats.containerStats.containsKey(containerId); 

        if (containerRegistered) {
            containerStats = _stats.containerStats.get(containerId);
        } else {
            containerStats = new HashMap<>();
            containerStats.put("status", "IRRETRIEVABLE");
            containerStats.put("memory_limit", -1L);
            containerStats.put("memory_usage", -1L);
            containerStats.put("memory_max_usage", -1L);
            containerStats.put("num_procs", -1);
            containerStats.put("network_i", -1L);
            containerStats.put("network_o", -1L);
            containerStats.put("cpu_usage", -1);
            containerStats.put("container_id", containerId);
            containerStats.put("image_id", image); 
            _stats.containerStats.put(containerId, containerStats);
        }

        return IDUUIResource
            .getContainerStats(_interface, containerStats, containerId, image);
    }

    private class DockerStats {
        Map<String, AtomicInteger> crashes = new ConcurrentHashMap<>(20);
        Map<String, Map<String, Object>> 
            containerStats = new ConcurrentHashMap<>(); 
        Map<String, Object> dispatchMap = new HashMap<>();
        Map<String, Integer> _heightsMap = new ConcurrentHashMap<>();
        AtomicBoolean initialScaling = new AtomicBoolean(false);
    }

    public static class ComponentInstance implements IDUUIUrlAccessible {
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
        Set<ComponentInstance> _instancesSet; 
        DUUIPipelineComponent _component;
        String _uniqueComponentKey; 
        Signature _signature; 
        final DUUIDockerDriver _driver;        

        InstantiatedComponent(DUUIPipelineComponent comp, String uuid, DUUIDockerDriver driver) {
            _driver = driver;
            _component = comp;
            if (comp.getDockerImageName() == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _uniqueComponentKey = uuid;

            _instances = new LinkedBlockingQueue<ComponentInstance>();
            _instancesSet = ConcurrentHashMap.newKeySet();
        }

        public void addInstance(ComponentInstance inst) {
            _driver.pause(inst.getContainerId());
            _instances.offer(inst);
            _instancesSet.add(inst);

            if (getScale() < _instancesSet.size())
                _component.setScale(_instancesSet.size());
        }

        public void removeInstance() {
            ComponentInstance _instance = _instances.poll();
            if (_instance == null) return;
            _driver._interface.stop_container(_instance.getContainerId());
            _instancesSet.remove(_instance);
        }
        
        public void returnInstance(IDUUIUrlAccessible access) throws InterruptedException {
            boolean pauseSuccessful;
            pauseSuccessful = _driver.pause(((ComponentInstance)access).getContainerId());
            if (pauseSuccessful) {
                _instances.offer((ComponentInstance) access); // Only return uncrashed containers
                return;
            }

            System.out.printf("[%s][%s] CONTAINER CRASHED %s", 
                Thread.currentThread().getName(), 
                this.getClass().getSimpleName(),
                getImageName());
            _driver._stats.crashes.get(_uniqueComponentKey).incrementAndGet();
            _instancesSet.remove(access);

            if (_instancesSet.size() < 1) {
                boolean scaledUp = _driver.scaleUp(_uniqueComponentKey, 3);
                if (!scaledUp) {
                    System.out.printf("[%s][%s] SCALING UP FAILED %s", 
                        Thread.currentThread().getName(), 
                        this.getClass().getSimpleName(),
                        getImageName());
                    throw new InterruptedException(
                        format("[%s][%s] Component instances failed too many times: %s", 
                            Thread.currentThread().getName(), 
                            this.getClass().getSimpleName(),
                            getImageName())
                    );
                }
            }
        }

        @Override
        public IDUUIUrlAccessible takeInstance() throws InterruptedException {
            try {
                ComponentInstance url = getInstances().poll(
                        Config.strategy().getTimeout(TimeUnit.MILLISECONDS), 
                        TimeUnit.MILLISECONDS
                );

                boolean resumeSuccessful;
                if (url != null) {
                    resumeSuccessful = _driver.resume(url.getContainerId());
                    if (resumeSuccessful) // if false, container likely crashed or worse
                        return url; 
                    else {
                        System.out.printf("[%s][%s] CONTAINER CRASHED %s", 
                            Thread.currentThread().getName(), 
                            this.getClass().getSimpleName(),
                            getImageName());
                        _instancesSet.remove(url);
                    } // Remove crashed container
                } 
                
                if (_instancesSet.size() < getScale()){// Scale represents the maximum number of urls
                    boolean scaledUp = _driver.scaleUp(_uniqueComponentKey, 3);
                    if (!scaledUp) {
                        System.out.printf("[%s][%s] SCALING UP FAILED %s", 
                            Thread.currentThread().getName(), 
                            this.getClass().getSimpleName(),
                            getImageName());
                        throw new InterruptedException(
                            format("[%s][%s] Component instances failed too many times: %s", 
                                Thread.currentThread().getName(), 
                                this.getClass().getSimpleName(),
                                getImageName())
                        );
                    }
                }
                
                url = getInstances().take();
                resumeSuccessful = _driver.resume(url.getContainerId());
                if (!resumeSuccessful) {
                    System.out.printf("[%s][%s] CONTAINER CRASHED %s", 
                        Thread.currentThread().getName(), 
                        this.getClass().getSimpleName(),
                        getImageName());
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
