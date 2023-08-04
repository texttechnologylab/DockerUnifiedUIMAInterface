package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import java.net.*;
import java.util.Collections;
import java.util.Optional;


import static io.fabric8.kubernetes.client.impl.KubernetesClientImpl.logger;
import static java.lang.String.format;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
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
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.Map;

public class DUUIKubernetesDriver implements IDUUIDriverInterface {

    private KubernetesClient _kube_client;

    private HashMap<String, InstantiatedComponent> _active_components;
    private DUUILuaContext _luaContext;
    private final DUUIDockerInterface _interface;
    private HttpClient _client;
    private int _container_timeout;

    private IDUUIConnectionHandler _wsclient;

    public DUUIKubernetesDriver() throws IOException {
        _interface = new DUUIDockerInterface();

        _container_timeout = 10000;
        _client = HttpClient.newHttpClient();

        _active_components = new HashMap<>();

        //_kube_client = new DefaultKubernetesClient();
    }

    // Hier muss anscheinend nichts mehr gemacht werden
    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return component.getDockerImageName()!=null;
    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception {
        String uuid = UUID.randomUUID().toString();  // Erstelle ID für die neue Komponente.
        while (_active_components.containsKey(uuid.toString())) {  // Stelle sicher, dass ID nicht bereits existiert (?)
            uuid = UUID.randomUUID().toString();
        }
        DUUIKubernetesDriver.InstantiatedComponent comp = new DUUIKubernetesDriver.InstantiatedComponent(component);  // Initialisiere Komponente

        String dockerImage = comp.getImageName();  // Image der Komponente als String
        int scale = comp.getScale(); // Anzahl der Replicas in dieser Kubernetes-Komponente

        KubernetesClient k8s = new KubernetesClientBuilder().build();

        if(!isMasterNode(k8s)) {
            throw new InvalidParameterException("This node is not a Kubernetes Master Node, thus cannot create and schedule new services!");
        }

        Service service;
        try {
            // Füge dem Namen vorne "a" hinzu, weil laut Regel die Namen mit alphabetischem Zeichen beginnen müssen (darf nicht mit Ziffer beginnen)
            createDeployment("a"+uuid, dockerImage, scale);  // Erstelle Deployment
            service = createService("a"+uuid);  // Erstelle service und gebe diesen zurück
        }
        catch (Exception e){
            deleteDeployment("a"+uuid);
            deleteService("a"+uuid);
            throw e;
        }

        int port = service.getSpec().getPorts().get(0).getNodePort();  // NodePort

        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = null;

        try {
            // TODO: Kann man hier wirklich einfach die DockerDriver-Methode verwenden?
            layer = DUUIDockerDriver.responsiveAfterTime("http://localhost:" + port, jc, _container_timeout, _client, (msg) -> {
                System.out.printf("[KubernetesDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(), msg);
            }, _luaContext, skipVerification);
        }
        catch (Exception e){
            deleteDeployment("a"+uuid);
            deleteService("a"+uuid);
            throw e;
        }


        System.out.printf("[KubernetesDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(),comp.getImageName(), port);

        comp.initialise(port, layer, this);
        Thread.sleep(500);


        _active_components.put(uuid, comp);
        return uuid;
    }

    public boolean isMasterNode(KubernetesClient kubeClient) throws SocketException {
        String masterNodeIP = kubeClient.getMasterUrl().getHost();  // IP-Adresse des Master Node
        Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
        // Quelle des Codeschnipsels: https://www.educative.io/answers/how-to-get-the-ip-address-of-a-localhost-in-java
        while( networkInterfaceEnumeration.hasMoreElements()){
            for ( InterfaceAddress interfaceAddress : networkInterfaceEnumeration.nextElement().getInterfaceAddresses()) {
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
     * Erstellt Deployment für Kubernetes Cluster.
     * @param image
     * @param replicas
     */
    public static void createDeployment(String name, String image, int replicas) {
        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            // Load Deployment YAML Manifest into Java object
            Deployment deployment = new DeploymentBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .endMetadata()
                    .withNewSpec()
                    .withReplicas(replicas)
                    .withNewTemplate()
                    .withNewMetadata()
                    .addToLabels("app", "nginx")  // selector label
                    .endMetadata()
                    .withNewSpec()
                    .addNewContainer()
                    .withName("nginx")
                    .withImage(image)
                    .addNewPort()
                    .withContainerPort(80)
                    .endPort()
                    .endContainer()
                    .endSpec()
                    .endTemplate()
                    .withNewSelector()
                    .addToMatchLabels("app", "nginx")
                    .endSelector()
                    .endSpec()
                    .build();

            deployment = k8s.apps().deployments().inNamespace("default").resource(deployment).create();
        }
    }

    /**
     * Erstellt Service für Kubernetes-Cluster
     */
    public static Service createService(String name) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            String namespace = Optional.ofNullable(client.getNamespace()).orElse("default");
            Service service = new ServiceBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .endMetadata()
                    .withNewSpec()
                    .withSelector(Collections.singletonMap("app", "nginx"))
                    .addNewPort()
                    .withName("test-port")
                    .withProtocol("TCP")
                    .withPort(80)
                    .withTargetPort(new IntOrString(9714))  // TargetPort muss anscheinend auf 9714 gesetzt werden.
                    .endPort()
                    .withType("LoadBalancer")
                    .endSpec()
                    .build();

            service = client.services().inNamespace(namespace).resource(service).create();
            logger.info("Created service with name {}", service.getMetadata().getName());

            String serviceURL = client.services().inNamespace(namespace).withName(service.getMetadata().getName())
                    .getURL("test-port");
            logger.info("Service URL {}", serviceURL);

            return service;
        }
    }

    /**
     * Löscht Deployment
     */
    public static void deleteDeployment(String name) {
        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            // argument namespace könnte noch verallgemeinert werden
            k8s.apps().deployments().inNamespace("default")
                    .withName(name)
                    .delete();
        }
    }

    /**
     * Löscht Service.
     */
    public static void deleteService(String name) {
        try (KubernetesClient client = new DefaultKubernetesClient()) {
            // Argument namespace könnte noch verallgemeinert werden.
            client.services().inNamespace("default").withName(name).delete();
        }
    }

    // Kann man sich schenken
    @Override
    public void printConcurrencyGraph(String uuid) {

    }

    @Override
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUIKubernetesDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid,comp);
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException, CASException {
        DUUIKubernetesDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }

        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        }
        else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }

    @Override
    public void destroy(String uuid) {
        DUUIKubernetesDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            deleteDeployment("a"+uuid);
            deleteService("a"+uuid);
        }
    }


    @Override
    public void shutdown() {

    }

    // Das ist eine Instanz innerhalb einer Komponente. Beispielsweise kann eine Kubernetes-Komponente
    // ja aus mehreren Pods bestehen. Diese Pods werden dann durch diese Klasse hier repräsentiert.
    public static class ComponentInstance implements IDUUIUrlAccessible {
        private String _pod_ip;
        private IDUUIConnectionHandler _handler;
        private IDUUICommunicationLayer _communicationLayer;

        public ComponentInstance(String pod_ip, IDUUICommunicationLayer communicationLayer) {
            _pod_ip = pod_ip;
            _communicationLayer = communicationLayer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        public ComponentInstance(String pod_ip, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _pod_ip = pod_ip;
            _communicationLayer = layer;
            _handler = handler;
        }

        @Override
        public String generateURL() {
            return _pod_ip;
        }

        public IDUUIConnectionHandler getHandler() {return _handler;}
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {

        private String _image_name;
        private int _service_port;
        private boolean _gpu;
        private boolean _keep_running_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private Map<String,String> _parameters;
        private DUUIPipelineComponent _component;

        private final boolean _websocket;

        private int _ws_elements;  // Dieses Attribut wird irgendwie dem _wsclient-String am Ende angeheftet. Ka wieso.

        private final ConcurrentLinkedQueue<DUUIKubernetesDriver.ComponentInstance> _components;

        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _image_name = comp.getDockerImageName();
            _parameters = comp.getParameters();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }
            _withImageFetching = comp.getDockerImageFetching(false);


            _scale = comp.getScale(1);

            _gpu = comp.getDockerGPU(false);

            _keep_running_after_exit = comp.getDockerRunAfterExit(false);

            _components = new ConcurrentLinkedQueue<>();

            _ws_elements = comp.getWebsocketElements();

            _websocket = comp.isWebsocket();
        }

        // Implementiert, weil SwarmDriver auch so eine Methode hat.
        public DUUIKubernetesDriver.InstantiatedComponent initialise(int service_port, IDUUICommunicationLayer layer, DUUIKubernetesDriver kubeDriver) throws IOException, InterruptedException {
            _service_port = service_port;

            if (_websocket) {
                kubeDriver._wsclient = new DUUIWebsocketAlt(
                        getServiceUrl().replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, _ws_elements);
            }
            else {
                kubeDriver._wsclient = null;
            }
            for(int i = 0; i < _scale; i++) {
                _components.add(new DUUIKubernetesDriver.ComponentInstance(getServiceUrl(), layer.copy(), kubeDriver._wsclient));

            }
            return this;
        }

        public String getServiceUrl() {
            return format("http://localhost:%d",_service_port);  // _service_port ist der NodePort des services, also das im Bereich 30.000
        }

        @Override
        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        // vorläufig
        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            DUUIKubernetesDriver.ComponentInstance inst = _components.poll();
            while(inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst,mutexStart,mutexEnd);
        }

        @Override
        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }

        @Override
        public Map<String, String> getParameters() {
            return null;
        }

        @Override
        public String getUniqueComponentKey() {
            return null;
        }

        public boolean getRunningAfterExit() {
            return _keep_running_after_exit;
        }

        public String getImageName() {
            return _image_name;
        }

        public int getScale() {
            return _scale;
        }

        public void set_service_port(int servicePort) {
            this._service_port = servicePort;
        }

        public int getWebsocketElements() { return _ws_elements; }

        public boolean isWebsocket() {
            return _websocket;
        }
    }

    // Dieses Objekt wird in die Composer.add-Methode eingegeben und so zum _Pipeline-Attribut des Composers hinzugefügt.
    public static class Component {
        private DUUIPipelineComponent _component;  // Dieses Attribut wird letztlich der Methode "instantiate" übergeben.

        public Component(String globalRegistryImageName) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(globalRegistryImageName);
        }

        public DUUIKubernetesDriver.Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIKubernetesDriver.class);
            return _component;
        }
    }
}
