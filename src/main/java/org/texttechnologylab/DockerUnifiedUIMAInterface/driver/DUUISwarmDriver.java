package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.IDUUICommunicationLayer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;

public class DUUISwarmDriver implements IDUUIConnectedDriverInterface {
    private final DUUIDockerInterface _interface;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;
    private final Map<String, IDUUIInstantiatedPipelineComponent> _active_components;
    private int _container_timeout;
    private DUUILuaContext _luaContext;
    private String _withSwarmVisualizer;


    public DUUISwarmDriver() throws IOException {
        _interface = new DUUIDockerInterface();

        _container_timeout = 10000;
        _client = HttpClient.newHttpClient();

        _active_components = new HashMap<>();
        _withSwarmVisualizer = null;
    }

    public DUUISwarmDriver(int timeout) throws IOException, UIMAException {
        _interface = new DUUIDockerInterface();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _container_timeout = timeout;
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _active_components = new ConcurrentHashMap<>();
    }

    public Map<String, IDUUIInstantiatedPipelineComponent> getComponents() {
        return _active_components;
    }

    public DUUISwarmDriver withSwarmVisualizer() throws InterruptedException {
        return withSwarmVisualizer(null);
    }

    public DUUISwarmDriver withSwarmVisualizer(Integer port) throws InterruptedException {
        if(_withSwarmVisualizer==null) {
            _interface.pullImage("dockersamples/visualizer",null,null);
            if(port == null) {
                _withSwarmVisualizer = _interface.run("dockersamples/visualizer",false,true,8080,true);
            }
            else {
                _withSwarmVisualizer = _interface.run("dockersamples/visualizer",false,true,8080,port,true);
            }
            int port_mapping = _interface.extract_port_mapping(_withSwarmVisualizer,8080);
            System.out.printf("[DUUISwarmDriver] Running visualizer on address http://localhost:%d\n",port_mapping);
            Thread.sleep(1500);
        }
        return this;
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    DUUISwarmDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    public boolean canAccept(DUUIPipelineComponent comp) {
        try {
            InstantiatedComponent s = new InstantiatedComponent(comp);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public void printConcurrencyGraph(String uuid) {
        IDUUIInstantiatedPipelineComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerSwarmDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
    }

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        if(!_interface.isSwarmManagerNode()) {
            throw new InvalidParameterException("This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);

        if(_interface.getLocalImage(comp.getImageName()) == null) {
            // If image is not available try to pull it
            _interface.pullImage(comp.getImageName(),null,null);
        }

        if(comp.isBackedByLocalImage()) {
            System.out.printf("[DockerSwarmDriver] Attempting to push local image %s to remote image registry %s\n", comp.getLocalImageName(),comp.getImageName());
            if(comp.getUsername() != null && comp.getPassword() != null) {
                System.out.println("[DockerSwarmDriver] Using provided password and username to authentificate against the remote registry");
            }
            _interface.push_image(comp.getImageName(),comp.getLocalImageName(),comp.getUsername(),comp.getPassword());
        }
        System.out.printf("[DockerSwarmDriver] Assigned new pipeline component unique id %s\n", uuid);

        String digest = _interface.getDigestFromImage(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(),digest);
        System.out.printf("[DockerSwarmDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(),digest);

        String serviceid = _interface.run_service(digest,comp.getScale(),comp.getConstraints());

        int port = _interface.extract_service_port_mapping(serviceid);

        System.out.printf("[DockerSwarmDriver][%s] Started service, waiting for it to become responsive...\n",uuid);

        if (port == 0) {
            throw new UnknownError("Could not read the service port!");
        }
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;
        try {
                layer = get_communication_layer("http://localhost:" + port, jc, _container_timeout, _client, (msg) -> {
                System.out.printf("[DockerSwarmDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(), msg);
            }, _luaContext, skipVerification);
        }
        catch (Exception e){
            _interface.rm_service(serviceid);
            throw e;
        }

        System.out.printf("[DockerSwarmDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(),comp.getImageName(), port);

        comp.initialise(serviceid,port, layer, this);
        Thread.sleep(500);

        _active_components.put(uuid, comp);
        return uuid;
    }

    public void destroy(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Swarm Driver");
        }
        if (!comp.getRunningAfterExit()) {
            System.out.printf("[DockerSwarmDriver] Stopping service %s...\n",comp.getServiceId());
            _interface.rm_service(comp.getServiceId());
        }
    }

    public void shutdown() {
        if(_withSwarmVisualizer!=null) {
            System.out.println("[DUUISwarmDriver] Shutting down swarm visualizer now!");
            _interface.stop_container(_withSwarmVisualizer);
            _withSwarmVisualizer = null;
        }
    }
    
    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;
        IDUUIConnectionHandler _handler;
        IDUUICommunicationLayer _communication_layer;

        public ComponentInstance(String url, IDUUICommunicationLayer layer) {
            _url = url;
            _communication_layer = layer;
        }

        public ComponentInstance(String url, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _url = url;
            _communication_layer = layer;
            _handler = handler;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public String generateURL() {
            return _url;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        
        private String _service_id;
        private int _service_port;
        private final String _fromLocalImage;
        private final ConcurrentLinkedQueue<ComponentInstance> _components;
        private DUUIPipelineComponent _component;
        private Signature _signature; 

        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            if (comp.getDockerImageName() == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _components = new ConcurrentLinkedQueue<>();
            _fromLocalImage = null;
        }

        public InstantiatedComponent initialise(String service_id, int container_port, IDUUICommunicationLayer layer, DUUISwarmDriver swarmDriver) throws IOException, InterruptedException {

            _service_id = service_id;
            _service_port = container_port;

            if (isWebsocket()) {
                String ws_url = getServiceUrl().replaceFirst("http", "ws");
                swarmDriver._wsclient = new DUUIWebsocketAlt(
                        ws_url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET,
                        getWebsocketElements());
            }
            else {
                swarmDriver._wsclient = null;
            }
            for(int i = 0; i < getScale(); i++) {
                _components.add(new ComponentInstance(getServiceUrl(), layer.copy(), swarmDriver._wsclient));

            }
            
            return this;
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return "";
        }
                
        public ConcurrentLinkedQueue<ComponentInstance> getInstances() {
            return _components;
        }

        public boolean isBackedByLocalImage() {
            return _fromLocalImage!=null;
        }

        public String getLocalImageName() {
            return _fromLocalImage;
        }

        public String getServiceUrl() {
            return format("http://localhost:%d",_service_port);
        }

        public String getServiceId() {
            return _service_id;
        }

        public int getServicePort() {
            return _service_port;
        }

        public String getPassword() {
            return _component.getDockerAuthPassword();
        }

        public String getUsername() {
            return _component.getDockerAuthUsername();
        }

        public List<String> getConstraints() {
            return _component.getConstraints();
        }

        public String getImageName() {
            return _component.getDockerImageName();
        }

        public boolean getRunningAfterExit() {
            return _component.getDockerRunAfterExit(false);
        }

        @Override
        public void setSignature(Signature sig) {
            _signature = sig;  
        }

        @Override
        public Signature getSignature() {
            return _signature; 
        }

        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }
    }

    public static class Component implements IDUUIDriverComponent<Component> {
        private DUUIPipelineComponent component;

        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withDockerImageName(globalRegistryImageName);
        }

        public Component(DUUIPipelineComponent pComponent) {
            component = pComponent;
        }

        public Component getComponent() {
            return this;
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return component;
        }

        public Component withConstraintHost(String sHost){
            component.withConstraint("node.hostname=="+sHost);
            return this;
        }
        
        public Component withConstraintLabel(String sKey, String sValue){
            component.withConstraint("node.labels."+sKey+"=="+sValue);
            return this;
        }

        public Component withConstraints(List<String> constraints) {
            component.withConstraints(constraints);
            return this;
        }

        public Component withRegistryAuth(String username, String password) {
            component.withDockerAuth(username,password);
            return this;
        }

        public Component withRunningAfterDestroy(boolean run) {
            component.withDockerRunAfterExit(run);
            return this;
        }

        public Component withWebsocket(boolean b) {
            component.withWebsocket(b);
            return this;
        }

        public Component withWebsocket(boolean b, int elements) {
            component.withWebsocket(b, elements);
            return this;
        }

        public DUUIPipelineComponent build() {
            component.withDriver(DUUISwarmDriver.class);
            return component;
        }
    }
}
