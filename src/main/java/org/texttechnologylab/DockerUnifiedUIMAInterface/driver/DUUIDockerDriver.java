package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import static java.lang.String.format;

public class DUUIDockerDriver implements IDUUIConnectedDriverInterface {
    private DUUIDockerInterface _interface;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;

    private Map<String, IDUUIInstantiatedPipelineComponent> _active_components;
    private int _container_timeout;
    private DUUILuaContext _luaContext;

    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUIDockerDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newHttpClient();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _container_timeout = 10000;


        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _active_components = new HashMap<>();
        _luaContext = null;
    }

    public DUUIDockerDriver(int timeout) throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

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
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component);

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
        for (int i = 0; i < comp.getScale(); i++) {
            String containerid = _interface.run(comp.getPipelineComponent().getDockerImageName(), comp.usesGPU(), true, 9714,false);
            int port = _interface.extract_port_mapping(containerid);

            try {
                if (port == 0) {
                    throw new UnknownError("Could not read the container port!");
                }
                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = get_communication_layer("http://127.0.0.1:" + String.valueOf(port), jc, _container_timeout, _client,(msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                },_luaContext, skipVerification);
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), port);

                if (comp.isWebsocket()) {
                    String url = "ws://127.0.0.1:" + String.valueOf(port);
                    _wsclient = new DUUIWebsocketAlt(
                            url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, comp.getWebsocketElements());
                }
                else {
                    _wsclient = null;
                }

                comp.addInstance(new ComponentInstance(containerid, port, layer, _wsclient));
            }
            catch(Exception e) {
                //_interface.stop_container(containerid);
                //throw e;
            }
        }
        return uuid;
    }

    public void destroy(String uuid) {
        InstantiatedComponent comp = (InstantiatedComponent) _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local driver.");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (ComponentInstance inst : comp.getInstances()) {
                System.out.printf("[DockerLocalDriver][Replication %d/%d] Stopping docker container %s...\n",counter,comp.getInstances().size(),inst.getContainerId());
                _interface.stop_container(inst.getContainerId());
                counter+=1;
            }
        }
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
        private ConcurrentLinkedQueue<ComponentInstance> _instances;
        private DUUIPipelineComponent _component;
        private String _uniqueComponentKey; 

        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            if (comp.getDockerImageName() == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _uniqueComponentKey = "";

            _instances = new ConcurrentLinkedQueue<ComponentInstance>();
        }

        public void addComponent(IDUUIUrlAccessible access) {
            _instances.add((ComponentInstance) access);
        }

        public void addInstance(ComponentInstance inst) {
            _instances.add(inst);
        }
        
        public ConcurrentLinkedQueue<ComponentInstance> getInstances() {
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
