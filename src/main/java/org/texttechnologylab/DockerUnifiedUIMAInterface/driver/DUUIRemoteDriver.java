package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUICompressionHelper;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DUUIRemoteDriver implements IDUUIConnectedDriver{
    private Map<String, IDUUIInstantiatedPipelineComponent> _components;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient = null;
    private DUUICompressionHelper _helper;
    private DUUILuaContext _luaContext;



    public DUUIRemoteDriver(int timeout) {
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();
        _components = new ConcurrentHashMap<>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public DUUIRemoteDriver() {
        _components = new HashMap<>();
        _client = HttpClient.newHttpClient();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }
    
    public Map<String, ? extends IDUUIInstantiatedPipelineComponent> getComponents() {
        return _components;
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    public boolean canAccept(DUUIPipelineComponent component) {
        List<String> urls = component.getUrl();
        return urls != null && urls.size() > 0;
    }

    public void printConcurrencyGraph(String uuid) {
        IDUUIInstantiatedPipelineComponent component = _components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[RemoteDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
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
            if (comp.isWebsocket()) {
                String websocketUrl = url.replaceFirst("http", "ws")
                        + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET;
                _wsclient = new DUUIWebsocketAlt(websocketUrl, comp.getWebsocketElements());
            }
            IDUUICommunicationLayer layer = get_communication_layer(url, jc, 100000, _client, (msg) -> {
                System.out.printf("[RemoteDriver][%s] %s\n", uuidCopy, msg);
            }, _luaContext, skipVerification);
            
            if(!added_communication_layer) {
                added_communication_layer = true;
            }
            for (int i = 0; i < comp.getScale(); i++) {
                comp.addComponent(new ComponentInstance(url,layer.copy(), _wsclient));
            }
            _components.put(uuid, comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, url);

            System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getScale());
        }

        return uuid;
    }

    public void destroy(String uuid) {
        _components.remove(uuid);
    }

    public static class Component implements IDUUIDriverComponent<Component> {
        DUUIPipelineComponent component;

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

        public Component getComponent() {
            return this;
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return component;
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
            component.withDriver(DUUIRemoteDriver.class);
            return component;
        }
    }

    static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;
        IDUUIConnectionHandler _handler;
        IDUUICommunicationLayer _communication_layer;

        ComponentInstance(String val, IDUUICommunicationLayer layer) {
            _url = val;
            _communication_layer = layer;
            _handler = null;
        }

        ComponentInstance(String val,  IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _url = val;
            _handler = handler;
            _communication_layer = layer;
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
    
    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        List<String> _urls;
        BlockingQueue<ComponentInstance> _components;
        ResourceManager _manager;
        String _uniqueComponentKey;
        DUUIPipelineComponent _component;
        Signature _signature; 

        public InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _urls = comp.getUrl();
            if (_urls == null || _urls.size() == 0) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }

            _uniqueComponentKey = "";

            _components = new LinkedBlockingQueue<ComponentInstance>(getScale());
        }

        public void addComponent(IDUUIUrlAccessible access) {
            _components.add((ComponentInstance) access);
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public void setResourceManager(ResourceManager manager) {
            _manager = manager;
        }

        public ResourceManager getResourceManager() {
            return _manager; 
        }

        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }
                        
        public BlockingQueue<ComponentInstance> getInstances() {
            return _components;
        }

        public List<String> getUrls() {
            return _component.getUrl();
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

}
