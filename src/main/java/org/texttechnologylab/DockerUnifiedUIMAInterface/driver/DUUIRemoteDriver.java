package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUICompressionHelper;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DUUIRemoteDriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _components;
    private HttpClient _client;
    private DUUICompressionHelper _helper;
    private DUUILuaContext _luaContext;


    public static class Component extends IDUUIPipelineComponent {
        public Component(String url) {
            setOption("url", url);
        }

        public Component withScale(int scale) {
            setOption("scale", String.valueOf(scale));
            return this;
        }
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _url;

        ComponentInstance(String val) {
            _url = val;
        }

        public String generateURL() {
            return _url;
        }
    }
    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _url;
        private int _maximum_concurrency;
        private ConcurrentLinkedQueue<ComponentInstance> _components;
        private IDUUICommunicationLayer _layer;
        private String _uniqueComponentKey;
        private Map<String,String> _parameters;

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _layer;
        }
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

        public void setCommunicationLayer(IDUUICommunicationLayer layer) {
            _layer = layer;
        }

        public InstantiatedComponent(IDUUIPipelineComponent comp) {
            _url = comp.getOption("url");
            if (_url == null) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }
            _parameters = comp.getParameters();

            _uniqueComponentKey = comp.getOption(DUUIComposer.COMPONENT_COMPONENT_UNIQUE_KEY);

            String max = comp.getOption("scale");
            _components = new ConcurrentLinkedQueue<>();
            if(max != null) {
                _maximum_concurrency = Integer.valueOf(max);
            }
            else {
                _maximum_concurrency = 1;
            }
        }

        public String getUniqueComponentKey() {return _uniqueComponentKey;}

        public int getScale() {
            return _maximum_concurrency;
        }

        public String getUrl() {
            return _url;
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

    public boolean canAccept(IDUUIPipelineComponent component) {
        return component.getClass().getCanonicalName() == component.getClass().getCanonicalName();
    }

    public void shutdown() {
    }

    public String instantiate(IDUUIPipelineComponent component, JCas jc, boolean skipVerification) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);

        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = DUUIDockerDriver.responsiveAfterTime(comp.getUrl(), jc, 100000, _client, (msg) -> {
            System.out.printf("[RemoteDriver][%s] %s\n", uuidCopy,msg);
        },_luaContext, skipVerification);
        comp.setCommunicationLayer(layer);
        for(int i = 0; i < comp.getScale(); i++) {
            comp.addComponent(new ComponentInstance(comp.getUrl()));
        }
        _components.put(uuid, comp);
        System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, comp.getUrl());
        System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getScale());
        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[RemoteDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUIRemoteDriver.InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid,comp);
    }

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, CompressorException {
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
