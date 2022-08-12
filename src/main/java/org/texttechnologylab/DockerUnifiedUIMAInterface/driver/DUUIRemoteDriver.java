package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUICompressionHelper;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.InputsOutputs;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DUUIRemoteDriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _components;
    private HttpClient _client;
    private DUUICompressionHelper _helper;
    private DUUILuaContext _luaContext;


    public static class Component {
        private DUUIPipelineComponent component;
        public Component(String url) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withUrl(url);
        }

        public Component(List<String> urls) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withUrls(urls);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            component = pComponent;
        }

        public Component withScale(int scale) {
            component.withScale(scale);
            return this;
        }

        public Component withDescription(String description) {
            component.withDescription(description);
            return this;
        }

        public Component withParameter(String key, String value) {
            component.withParameter(key,value);
            return this;
        }

        public DUUIPipelineComponent build() {
            component.withDriver(DUUIRemoteDriver.class);
            return component;
        }
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;
        IDUUICommunicationLayer _communication_layer;

        ComponentInstance(String val, IDUUICommunicationLayer layer) {
            _url = val;
            _communication_layer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public String generateURL() {
            return _url;
        }
    }
    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private List<String> _urls;
        private int _maximum_concurrency;
        private ConcurrentLinkedQueue<ComponentInstance> _components;
        private String _uniqueComponentKey;
        private Map<String,String> _parameters;
        private DUUIPipelineComponent _component;

        public Triplet<IDUUIUrlAccessible,Long,Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while(inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst,mutexStart,mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }


        public InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _urls = comp.getUrl();
            if (_urls == null || _urls.size() == 0) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }

            _parameters = comp.getParameters();

            _uniqueComponentKey = "";

            _maximum_concurrency = comp.getScale(1);
            _components = new ConcurrentLinkedQueue<>();
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {return _uniqueComponentKey;}

        public int getScale() {
            return _maximum_concurrency;
        }

        public List<String> getUrls() {
            return _urls;
        }

        public Map<String,String> getParameters() {return _parameters;}
    }

    public DUUIRemoteDriver(int timeout) {
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _components = new HashMap<String, InstantiatedComponent>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public DUUIRemoteDriver() {
        _components = new HashMap<String, InstantiatedComponent>();
        _client = HttpClient.newHttpClient();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public boolean canAccept(DUUIPipelineComponent component) {
        List<String> urls = component.getUrl();
        return urls != null && urls.size() > 0;
    }

    public void shutdown() {
    }

    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);

        final String uuidCopy = uuid;
        boolean added_communication_layer = false;

        for(String url : comp.getUrls()) {
            IDUUICommunicationLayer layer = DUUIDockerDriver.responsiveAfterTime(url, jc, 100000, _client, (msg) -> {
                System.out.printf("[RemoteDriver][%s] %s\n", uuidCopy, msg);
            }, _luaContext, skipVerification);
            // Request to get input_output
            // {"inputs": ["de.sentence.tudarmstadt",...], "outputs": ["de.sentence.token",...]}
            // /v1/details/input_output
            if(!added_communication_layer) {
                added_communication_layer = true;
            }
            for (int i = 0; i < comp.getScale(); i++) {
                comp.addComponent(new ComponentInstance(url,layer.copy()));
            }
            _components.put(uuid, comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, url);
            System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getScale());
        }
        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[RemoteDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
    }

    @Override
    public InputsOutputs getInputsOutputs() {
        // TODO implement
        return null;
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUIRemoteDriver.InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid,comp);
    }

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, CompressorException, CASException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        IDUUIInstantiatedPipelineComponent.process(aCas,comp,perf);
    }

    public void destroy(String uuid) {
        _components.remove(uuid);
    }
}
