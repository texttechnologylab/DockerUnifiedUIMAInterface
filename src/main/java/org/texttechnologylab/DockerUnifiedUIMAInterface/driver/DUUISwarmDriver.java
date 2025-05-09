package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUISwarmDriver implements IDUUIDriverInterface {
    private final DUUIDockerInterface _interface;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;
    private final HashMap<String, DUUISwarmDriver.InstantiatedComponent> _active_components;
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

        _active_components = new HashMap<>();
    }


    public DUUISwarmDriver withSwarmVisualizer() throws InterruptedException {
        return withSwarmVisualizer(null);
    }

    public DUUISwarmDriver withSwarmVisualizer(Integer port) throws InterruptedException {
        if (_withSwarmVisualizer == null) {
            _interface.pullImage("dockersamples/visualizer", null, null);
            if (port == null) {
                _withSwarmVisualizer = _interface.run("dockersamples/visualizer", false, true, 8080, true);
            } else {
                _withSwarmVisualizer = _interface.run("dockersamples/visualizer", false, true, 8080, port, true);
            }
            int port_mapping = _interface.extract_port_mapping(_withSwarmVisualizer, 8080);
            System.out.printf("[DUUISwarmDriver] Running visualizer on address http://localhost:%d\n", port_mapping);
            Thread.sleep(1500);
        }
        return this;
    }

    public void shutdown() {
        if (_withSwarmVisualizer != null) {
            System.out.println("[DUUISwarmDriver] Shutting down swarm visualizer now!");
            _interface.stop_container(_withSwarmVisualizer);
            _withSwarmVisualizer = null;
        }
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
        } catch (Exception e) {
            return false;
        }
    }

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        if (!_interface.isSwarmManagerNode()) {
            throw new InvalidParameterException("This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }
        DUUISwarmDriver.InstantiatedComponent comp = new DUUISwarmDriver.InstantiatedComponent(component);

        if (_interface.getLocalImage(comp.getImageName()) == null) {
            // If image is not available try to pull it
            _interface.pullImage(comp.getImageName(), null, null);
            if (shutdown.get()) {
                return null;
            }
        }

        if (comp.isBackedByLocalImage()) {
            System.out.printf("[DockerSwarmDriver] Attempting to push local image %s to remote image registry %s\n", comp.getLocalImageName(), comp.getImageName());
            if (comp.getUsername() != null && comp.getPassword() != null) {
                System.out.println("[DockerSwarmDriver] Using provided password and username to authentificate against the remote registry");
            }
            _interface.push_image(comp.getImageName(), comp.getLocalImageName(), comp.getUsername(), comp.getPassword());
        }
        System.out.printf("[DockerSwarmDriver] Assigned new pipeline component unique id %s\n", uuid);

        String digest = _interface.getDigestFromImage(comp.getImageName());
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
        System.out.printf("[DockerSwarmDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(), digest);

        // TODO: Fragen was das hier macht
        String serviceid = _interface.run_service(digest, comp.getScale(), comp.getConstraints());

        // TODO: Fragen was das hier macht
        int port = _interface.extract_service_port_mapping(serviceid);

        System.out.printf("[DockerSwarmDriver][%s] Started service, waiting for it to become responsive...\n", uuid);

        if (port == 0) {
            throw new UnknownError("Could not read the service port!");
        }
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;
        try {
            if (shutdown.get()) {
                return null;
            }
            // TODO: Hier brauche ich irgendeine Analoge Funktion für den KubernetesDriver
            layer = DUUIDockerDriver.responsiveAfterTime("http://localhost:" + port, jc, _container_timeout, _client, (msg) -> {
                System.out.printf("[DockerSwarmDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(), msg);
            }, _luaContext, skipVerification);
        } catch (Exception e) {
            _interface.rm_service(serviceid);
            throw e;
        }

        System.out.printf("[DockerSwarmDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(), comp.getImageName(), port);

        comp.initialise(serviceid, port, layer, this);
        Thread.sleep(500);

        _active_components.put(uuid, comp);

        return shutdown.get() ? null : uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        DUUISwarmDriver.InstantiatedComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerSwarmDriver][%s]: Maximum concurrency %d\n", uuid, component.getScale());
    }

    public TypeSystemDescription get_typesystem(String uuid) throws IOException, ResourceInitializationException {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }

    /**
     * init reader component
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
    }

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }

        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        } else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

    public boolean destroy(String uuid) {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Swarm Driver");
        }
        if (!comp.getRunningAfterExit()) {
            System.out.printf("[DockerSwarmDriver] Stopping service %s...\n", comp.getServiceId());
            _interface.rm_service(comp.getServiceId());
        }

        return true;
    }

    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;
        IDUUIConnectionHandler _handler;
        IDUUICommunicationLayer _communication_layer;

        public ComponentInstance(String url, IDUUICommunicationLayer layer) {
            _url = url;
            _communication_layer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public ComponentInstance(String url, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _url = url;
            _communication_layer = layer;
            _handler = handler;
        }

        public String generateURL() {
            return _url;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private final String _image_name;
        private String _service_id;
        private int _service_port;
        private final Boolean _keep_runnging_after_exit;
        private final int _scale;
        private final String _fromLocalImage;
        private final ConcurrentLinkedQueue<ComponentInstance> _components;
        private final boolean _websocket;
        private final int _ws_elements;

        private final List<String> _constraints = new ArrayList<>(0);

        private final String _reg_password;
        private final String _reg_username;
        private final Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;


        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _image_name = comp.getDockerImageName();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            _scale = comp.getScale(1);
            _constraints.addAll(comp.getConstraints());
            _components = new ConcurrentLinkedQueue<>();

            _keep_runnging_after_exit = comp.getDockerRunAfterExit(false);

            _fromLocalImage = null;
            _reg_password = comp.getDockerAuthPassword();
            _reg_username = comp.getDockerAuthUsername();

            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }


        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return "";
        }

        public String getPassword() {
            return _reg_password;
        }

        public String getUsername() {
            return _reg_username;
        }

        public boolean isBackedByLocalImage() {
            return _fromLocalImage != null;
        }

        public String getLocalImageName() {
            return _fromLocalImage;
        }

        public boolean isWebsocket() {
            return _websocket;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }


        public InstantiatedComponent initialise(String service_id, int container_port, IDUUICommunicationLayer layer, DUUISwarmDriver swarmDriver) throws IOException, InterruptedException {

            _service_id = service_id;
            _service_port = container_port;

            if (_websocket) {
                swarmDriver._wsclient = new DUUIWebsocketAlt(
                    getServiceUrl().replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, _ws_elements);
            } else {
                swarmDriver._wsclient = null;
            }
            for (int i = 0; i < _scale; i++) {
                _components.add(new ComponentInstance(getServiceUrl(), layer.copy(), swarmDriver._wsclient));

            }
            return this;
        }

        public String getServiceUrl() {
            return format("http://localhost:%d", _service_port);
        }


        public String getImageName() {
            return _image_name;
        }

        public String getServiceId() {
            return _service_id;
        }

        public int getServicePort() {
            return _service_port;
        }

        public int getScale() {
            return _scale;
        }

        public List<String> getConstraints() {
            return _constraints;
        }

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {return _sourceView; }

        public String getTargetView() {return _targetView; }

        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }
    }

    public static class Component {
        private DUUIPipelineComponent component;

        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withDockerImageName(globalRegistryImageName);
        }

        public Component(DUUIPipelineComponent pComponent) {
            component = pComponent;
        }

        public Component withDescription(String description) {
            component.withDescription(description);
            return this;
        }

        public Component withParameter(String key, String value) {
            component.withParameter(key, value);
            return this;
        }

        public Component withView(String viewName) {
            component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            component.withTargetView(viewName);
            return this;
        }

        public Component withScale(int scale) {
            component.withScale(scale);
            return this;
        }

        public Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public Component withConstraintHost(String sHost) {
            component.withConstraint("node.hostname==" + sHost);
            return this;
        }

        public Component withConstraintLabel(String sKey, String sValue) {
            component.withConstraint("node.labels." + sKey + "==" + sValue);
            return this;
        }

        public Component withConstraints(List<String> constraints) {
            component.withConstraints(constraints);
            return this;
        }

        public DUUISwarmDriver.Component withRegistryAuth(String username, String password) {
            component.withDockerAuth(username, password);
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
