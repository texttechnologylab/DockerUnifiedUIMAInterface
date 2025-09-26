package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.fabric8.kubernetes.client.impl.KubernetesClientImpl.logger;
import static java.lang.String.format;

/**
 * Driver for the running of components in Kubernetes
 *
 * @author Markos Genios, Filip Fitzermann
 */
public class DUUIKubernetesDriver implements IDUUIDriverInterface {

    private final KubernetesClient _kube_client;

    private HashMap<String, InstantiatedComponent> _active_components;
    private DUUILuaContext _luaContext;
    private final DUUIDockerInterface _interface;
    private HttpClient _client;
    private int _container_timeout;

    private IDUUIConnectionHandler _wsclient;

    private int iScaleBuffer = 0;

    private static int _port = 9715;
    private static String sNamespace = "default";

    /**
     * Constructor.
     *
     * @throws IOException
     * @author Markos Genios
     */
    public DUUIKubernetesDriver() throws IOException {
        _kube_client = new KubernetesClientBuilder().build();

        _interface = new DUUIDockerInterface();

        _container_timeout = 1000;
        _client = HttpClient.newHttpClient();

        _active_components = new HashMap<>();
    }

    public DUUIKubernetesDriver withScaleBuffer(int iValue) {
        this.iScaleBuffer = iValue;
        return this;
    }

    public DUUIKubernetesDriver withScaleBuffer() {
        this.iScaleBuffer = 1;
        return this;
    }

    public int getScaleBuffer() {
        return this.iScaleBuffer;
    }

    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return component.getDockerImageName() != null;
    }

    /**
     * Creates Deployment for the kubernetes cluster.
     *
     * @param name:     Name of the deployment
     * @param image:    Image that the pods are running
     * @param replicas: number of pods (or more general: threads) to be created
     * @param labels:   Use only gpu-servers with the specified labels.
     * @author Markos Genios
     */
    public static void createDeployment(String name, String image, int replicas, List<String> labels) {

        if (labels.isEmpty()) {
            labels = List.of("disktype=all");
            System.out.println("(KubernetesDriver) defaulting to label disktype=all");
        }

        List<NodeSelectorTerm> terms = getNodeSelectorTerms(labels);

//        Map tMap = new HashMap();
//        tMap.put("vke.volcengine.com/container-multiple-gpu", "1");

        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            // Load Deployment YAML Manifest into Java object
            Deployment deployment;
            deployment = new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("pipeline-uid", name)
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(name)
                .withImage(image)
                .addNewPort()
                    .withContainerPort(_port)
                .endPort()
                .endContainer()
                .withNewAffinity()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .addAllToNodeSelectorTerms(terms)
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .endAffinity()
                .endSpec()

                .endTemplate()
                .withNewSelector()
                .addToMatchLabels("pipeline-uid", name)
                .endSelector()
                .endSpec()
                .build();

            deployment = k8s.apps().deployments().inNamespace(sNamespace).resource(deployment).create();
        }
    }

    /**
     * Creates Service for kubernetes cluster which is matched by selector labels to the previously created deployment.
     *
     * @param name
     * @return
     */
    public static Service createService(String name) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            String namespace = Optional.ofNullable(client.getNamespace()).orElse(sNamespace);
            Service service = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withSelector(Collections.singletonMap("pipeline-uid", name))  // Has to match the label of the deployment.
                .addNewPort()
                .withName("k-port")
                .withProtocol("TCP")
                    .withPort(_port)
                .withTargetPort(new IntOrString(9714))
                .endPort()
                .withType("LoadBalancer")
                .endSpec()
                .build();

            service = client.services().inNamespace(namespace).resource(service).create();
            logger.info("Created service with name {}", service.getMetadata().getName());

            String serviceURL = client.services().inNamespace(namespace).withName(service.getMetadata().getName())
                .getURL("k-port");
            logger.info("Service URL {}", serviceURL);

            return service;
        }
    }

    /**
     * Creates a list of NodeSelectorTerms from a list of labels. If added to a deployment the pods are scheduled onto
     * any node that has one or multiple of the given labels.
     * Each label must be given in the format "key=value".
     *
     * @param rawLabels
     * @return {@code List<NodeSelectorTerm>}
     */
    public static List<NodeSelectorTerm> getNodeSelectorTerms(List<String> rawLabels) {
        List<NodeSelectorTerm> terms = new ArrayList<>();

//        Splits each label in string form at the "=" and adds the resulting strings into a new
//        NodeSelectorTerm as key value pairs.
        for (String rawLabel : rawLabels) {
            String[] l = rawLabel.split("=");
            NodeSelectorTerm term = new NodeSelectorTerm();
            NodeSelectorRequirement requirement = new NodeSelectorRequirement(l[0], "In", List.of(l[1]));
            term.setMatchExpressions(List.of(requirement));
            terms.add(term);
        }
        return terms;
    }

    /**
     * Checks, whether the used Server is the master-node of kubernetes cluster.
     * Note: Function can give false-negative results, therefore is not used in the working code.
     *
     * @param kubeClient
     * @return
     * @throws SocketException
     * @author Markos Genios
     */
    public boolean isMasterNode(KubernetesClient kubeClient) throws SocketException {
        String masterNodeIP = kubeClient.getMasterUrl().getHost();  // IP-Adresse des Master Node
        Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
        // Source of code snippet: https://www.educative.io/answers/how-to-get-the-ip-address-of-a-localhost-in-java
        while (networkInterfaceEnumeration.hasMoreElements()) {
            for (InterfaceAddress interfaceAddress : networkInterfaceEnumeration.nextElement().getInterfaceAddresses()) {
                if (interfaceAddress.getAddress().isSiteLocalAddress()) {
                    if (interfaceAddress.getAddress().getHostAddress().equals(masterNodeIP)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Creates Deployment and Service. Puts the new component, which includes the Pods with their image to the active components.
     *
     * @param component
     * @param jc
     * @param skipVerification
     * @param shutdown
     * @return
     * @throws Exception
     * @author Markos Genios
     */
    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();  // Erstelle ID für die neue Komponente.
        while (_active_components.containsKey(uuid.toString())) {  // Stelle sicher, dass ID nicht bereits existiert (?)
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);  // Initialisiere Komponente

        String dockerImage = comp.getImageName();  // Image der Komponente als String
        int scale = comp.getScale(); // Anzahl der Replicas in dieser Kubernetes-Komponente

        Service service;
        try {
            /**
             * Add "a" in front of the name, because according to the kubernetes-rules the names must start
             * with alphabetical character (must not start with digit)
             */
            createDeployment("a" + uuid, dockerImage, scale + getScaleBuffer(), comp.getLabels());  // Erstelle Deployment
            service = createService("a" + uuid);  // Erstelle service und gebe diesen zurück
        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
            throw e;
        }
        if (shutdown.get()) return null;

        int port = service.getSpec().getPorts().get(0).getNodePort();  // NodePort
//            service.getSpec().getPorts().forEach(p->{
//                System.out.println("Node-port: "+p.getNodePort());
//                System.out.println("Port: "+p.getPort());
//                System.out.println("Target-port: "+p.getTargetPort());
//            });
        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;

        try {
            System.out.println("Port " + port);
            layer = DUUIDockerDriver.responsiveAfterTime("http://localhost:" + port, jc, _container_timeout, _client, (msg) -> {
                System.out.printf("[KubernetesDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(), msg);
            }, _luaContext, skipVerification);
        } catch (Exception e) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
            throw e;
        }


        System.out.printf("[KubernetesDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(), comp.getImageName(), port);

        comp.initialise(port, layer, this);
        Thread.sleep(500);

        _active_components.put(uuid, comp);
        return shutdown.get() ? null : uuid;
    }


    /**
     * Deletes the Deployment from the kubernetes cluster.
     *
     * @author Markos Genios
     */
    public static void deleteDeployment(String name) {
        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            // Argument namespace could be generalized.
            k8s.apps().deployments().inNamespace(sNamespace)
                .withName(name)
                .delete();
        }
    }

    /**
     * Deletes the service from the kubernetes cluster.
     *
     * @author Markos Genios
     */
    public static void deleteService(String name) {
        try (KubernetesClient client = new DefaultKubernetesClient()) {
            // Argument namespace could be generalized.
            client.services().inNamespace(sNamespace).withName(name).delete();
        }
    }

    @Override
    public void printConcurrencyGraph(String uuid) {

    }

    @Override
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        InstantiatedComponent comp = _active_components.get(uuid);
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
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }

        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        } else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

    /**
     * Deletes both the deployment and the service from the kubernetes cluster, if they exist.
     *
     * @param uuid
     * @author Markos Genios
     */
    @Override
    public boolean destroy(String uuid) {
        InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            deleteDeployment("a" + uuid);
            deleteService("a" + uuid);
        }

        return true;
    }


    @Override
    public void shutdown() {

    }

    /**
     * Class to represent a kubernetes pod: An Instance to process an entire document.
     *
     * @author Markos Genios
     */
    public static class ComponentInstance implements IDUUIUrlAccessible {
        private String _pod_ip;
        private IDUUIConnectionHandler _handler;
        private IDUUICommunicationLayer _communicationLayer;

        /**
         * Constructor.
         *
         * @param pod_ip
         * @param communicationLayer
         */
        public ComponentInstance(String pod_ip, IDUUICommunicationLayer communicationLayer) {
            _pod_ip = pod_ip;
            _communicationLayer = communicationLayer;
        }

        /**
         * @return
         */
        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        /**
         * Constructor
         * Sets:
         *
         * @param pod_ip
         * @param layer
         * @param handler
         * @author Markos Genios
         */
        public ComponentInstance(String pod_ip, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _pod_ip = pod_ip;
            _communicationLayer = layer;
            _handler = handler;
        }

        @Override
        public String generateURL() {
            return _pod_ip;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {

        private String _image_name;
        private int _service_port;
        private boolean _gpu;
        private final ConcurrentLinkedQueue<ComponentInstance> _components;
        private boolean _keep_running_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;

        private final boolean _websocket;

        private int _ws_elements;  // Dieses Attribut wird irgendwie dem _wsclient-String am Ende angeheftet. Ka wieso.
        private List<String> _labels;
        private String _uniqueComponentKey = "";

        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _image_name = comp.getDockerImageName();
            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }
            _withImageFetching = comp.getDockerImageFetching(false);

            _scale = comp.getScale(1);

            _gpu = comp.getDockerGPU(false);

            _labels = comp.getConstraints();

            _keep_running_after_exit = comp.getDockerRunAfterExit(false);

            _components = new ConcurrentLinkedQueue<>();

            _ws_elements = comp.getWebsocketElements();

            _websocket = comp.isWebsocket();
        }

        public InstantiatedComponent initialise(int service_port, IDUUICommunicationLayer layer, DUUIKubernetesDriver kubeDriver) throws IOException, InterruptedException {
            _service_port = service_port;

            if (_websocket) {
                kubeDriver._wsclient = new DUUIWebsocketAlt(
                    getServiceUrl().replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, _ws_elements);
            } else {
                kubeDriver._wsclient = null;
            }
            for (int i = 0; i < _scale; i++) {
                _components.add(new ComponentInstance(getServiceUrl(), layer.copy(), kubeDriver._wsclient));

            }
            return this;
        }

        /**
         * @return Url of the kubernetes-service.
         */
        public String getServiceUrl() {
            return format("http://localhost:%d", _service_port);
        }

        /**
         * @return pipeline component.
         */
        @Override
        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        @Override
        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }

        @Override
        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {return _sourceView; }

        public String getTargetView() {return _targetView; }

        @Override
        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        public boolean getRunningAfterExit() {
            return _keep_running_after_exit;
        }

        /**
         * @return name of the image.
         */
        public String getImageName() {
            return _image_name;
        }

        /**
         * @return number of processes/threads/pods
         */
        public int getScale() {
            return _scale;
        }

        /**
         * sets the service port.
         *
         * @param servicePort
         */
        public void set_service_port(int servicePort) {
            this._service_port = servicePort;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }

        public boolean isWebsocket() {
            return _websocket;
        }

        /**
         * returns true, iff only gpu-servers are used.
         *
         * @return
         */
        public boolean getGPU() {
            return _gpu;
        }

        /**
         * Returns the labels, to which the pods must be assigned.
         *
         * @return
         */
        public List<String> getLabels() {
            return _labels;
        }
    }

    /**
     * Instance of this class is input to composer.add-method and is added to the _Pipeline-attribute of the composer.
     *
     * @author Markos Genios
     */
    public static class Component {
        private DUUIPipelineComponent _component;  // Dieses Attribut wird letztlich der Methode "instantiate" übergeben.

        /**
         * Constructor. Creates Instance of Class Component.
         *
         * @param globalRegistryImageName
         * @throws URISyntaxException
         * @throws IOException
         */
        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(globalRegistryImageName);
        }

        /**
         * If used, the Pods get assigned only to GPU-Servers with the specified label.
         *
         * @return
         */
        public Component withLabels(String... labels) {  // Can be extended to "String..." to use more than one label!
            _component.withConstraints(List.of(labels));
            return this;
        }

        public Component withLabels(List<String> labels) {  // Can be extended to "String..." to use more than one label!
            _component.withConstraints(labels);
            return this;
        }

        /**
         * Sets the number of processes.
         *
         * @param scale
         * @return
         */
        public Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

        public Component withParameter(String key, String value) {
            _component.withParameter(key, value);
            return this;
        }

        public Component withView(String viewName) {
            _component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            _component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            _component.withTargetView(viewName);
            return this;
        }

        /**
         * Builds the component.
         *
         * @return
         */
        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIKubernetesDriver.class);
            return _component;
        }

        public Component withName(String name) {
            _component.withName(name);
            return this;
        }
    }
}
