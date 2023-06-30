package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
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
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

//import io.fabric8.kubernetes.client.*;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.responsiveAfterTime;

public class DUUIKubernetesDriver implements IDUUIDriverInterface {

//    private KubernetesClient _kube_client;

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

//        _kube_client = new DefaultKubernetesClient();
    }

    // Hier muss anscheinend nichts mehr gemacht werden
    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent comp) throws InvalidXMLException, IOException, SAXException {
        return comp.getDockerImageName()!=null;
    }

    // Wird in der instantiate_pipeline-Methode, die sich in der run-Methode des DUUIComposers befindet, aufgerufen.
    // Gibt die ID der Instanz zurück und fügt die Instanz zum Attribut _active_components hinzu.
    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception {
        String uuid = UUID.randomUUID().toString();  // Erstelle ID für die neue Komponente.
        while (_active_components.containsKey(uuid.toString())) {  // Stelle sicher, dass ID nicht bereits existiert (?)
            uuid = UUID.randomUUID().toString();
        }

        // TODO if-Bedingung, die nachschaut, ob der aktuell verwendete Knoten der Master-Node ist (analog zu dem,
        // TODO was im SwarmDriver gemacht wurde)

        DUUIKubernetesDriver.InstantiatedComponent comp = new DUUIKubernetesDriver.InstantiatedComponent(component);  // Initialisiere Komponente
        String dockerImage = comp.getImageName();  // Image der Komponente als String
        int scale = comp.getScale(); // Anzahl der Replicas in dieser Kubernetes-Komponente

//        ApiClient client = Config.fromUrl("https://192.168.122.198:6443");  // Kubernetes Client
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);  // Keine Ahnung
        CoreV1Api api = new CoreV1Api(client);  // Keine Ahnung

        // Erstellt scale-viele Pods mit spezifiziertem Image.
        for (int i = 0; i < scale; i++) {
            V1Pod pod = new V1Pod()
                    .metadata(new V1ObjectMeta().name("pod-" + i))
                    .spec(new V1PodSpec()
                            .containers(List.of(new V1Container()
                                    .name("my-container")
                                    .image(dockerImage))));

            api.createNamespacedPod("default", pod, null, null, null, null);
        }

        // Liste der Pods
        V1PodList podList = api.listNamespacedPod("default", null, null, null, null, null, null, null, null, null, null);

        // Laufvariable, die letztendlich die für die Unterscheidung der Namen der Pods zuständig ist.
        int i = 0;
        // Für jeden Pod im Cluster
        // Hier wird der CommunicationLayer für jeden Pod erstellt (der Kommunziert dann wahrscheinlich mit der IP-Adresse dieses Ports)
        // und anschließend wird dann der entsprechende Pod als ComponentInstance in die instantiatedComponent-Klasse hinzugefügt.
        for (V1Pod pod : podList.getItems()) {
            String podIP = pod.getStatus().getPodIP();
            final int iCopy = i;
            final String uuidCopy = uuid;
            // Keine Ahnung ob hier das Attribut "_container_timeout" in irgendeiner Form Sinn ergibt.
            IDUUICommunicationLayer layer = responsiveAfterTime("http://127.0.0.1:" + podIP, jc, _container_timeout, _client,(msg) -> {
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
            },_luaContext, skipVerification);
            System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), podIP);
            String url = "ws://127.0.0.1:" + podIP;
            // Wahrscheinlich wird hier dem client gesagt, in welchen Port oder in welche Adresse er hinschauen soll, um sich die Daten oder sonst
            // was zu holen.
            _wsclient = new DUUIWebsocketAlt(
                    url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, comp.getWebsocketElements());
            comp.addComponent(new ComponentInstance(podIP, layer, _wsclient));  // Füge Teil-Komponente (Pod) zur Gesamtkomponente (InstantiatedComponent) hinzu.
            i++;
        }

        // TODO: Überprüfe, ob image vorhanden ist (wie bei den anderen Drivern) (Bis jetzt gehe davon aus, dass es vorhanden ist)

        // Das hier wird irgendwie auch in allen Drivern gemacht. Ist aus irgendeinem Grund notwendig.
        // Womöglich muss man beim Kubernetes-Driver aber was anderes machen, da dieser kein Docker-Interface benötigt (glaube ich)
        String digest = _interface.getDigestFromImage(comp.getImageName());  // Digest ist im Gegensatz zu Image-Name anscheinend eindeutig.
        comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(),digest);  // Modifiziere Komponente entsprechend.

        _active_components.put(uuid, comp);  // Füge Komponente hinzu.

        return uuid;
    }

    @Override
    public void printConcurrencyGraph(String uuid) {

    }

    @Override
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        return null;
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException, CompressorException, CASException {

    }

    @Override
    public void destroy(String uuid) {

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
            return null;
        }

        public IDUUIConnectionHandler getHandler() {return _handler;}
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {

        private String _image_name;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private Map<String,String> _parameters;
        private DUUIPipelineComponent _component;

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

            _keep_runnging_after_exit = comp.getDockerRunAfterExit(false);

            _components = new ConcurrentLinkedQueue<>();

            _ws_elements = comp.getWebsocketElements();
        }

        @Override
        public DUUIPipelineComponent getPipelineComponent() {
            return null;
        }

        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            return null;
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

        public String getImageName() {
            return _image_name;
        }

        public int getScale() {
            return _scale;
        }

        public int getWebsocketElements() { return _ws_elements; }
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
