package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static java.lang.String.format;

interface ResponsiveMessageCallback {
    public void operation(String message);
}

public class DUUIDockerDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;


    private HashMap<String, InstantiatedComponent> _active_components;
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
        _active_components = new HashMap<String, InstantiatedComponent>();
        _luaContext = null;
    }

    public DUUIDockerDriver(int timeout) throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _container_timeout = 10000;

        _active_components = new HashMap<String, InstantiatedComponent>();
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    public DUUIDockerDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    public static IDUUICommunicationLayer responsiveAfterTime(String url, JCas jc, int timeout_ms, HttpClient client, ResponsiveMessageCallback printfunc, DUUILuaContext context, boolean skipVerification) throws Exception {
        long start = System.currentTimeMillis();
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();
        boolean fatal_error = false;
        /****
         * Givara Ebo
         */

        while(true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                    .version(HttpClient.Version.HTTP_1_1)
                    .GET()
                    .build();
            try {
                HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                if (resp.statusCode()== 200) {
                    String body2 = new String(resp.body(), Charset.defaultCharset());
                    try {
                        printfunc.operation("Component lua communication layer, loading...");
                        System.out.printf("Got script %s\n",body2);
                        IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2,"requester",context);
                        layer = lua_com;
                        printfunc.operation("Component lua communication layer, loaded.");
                        break;
                    }
                    catch(Exception e) {
                        fatal_error = true;
                        e.printStackTrace();
                        throw new Exception("Component provided a lua script which is not runnable.");
                    }
                } else if (resp.statusCode() == 404) {
                    printfunc.operation("Component provided no own communication layer implementation using fallback.");
                    break;
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if (timeElapsed > timeout_ms) {
                    throw new TimeoutException(format("The Container did not provide one succesful answer in %d milliseconds",timeout_ms));
                }

            } catch (Exception e) {
                if(fatal_error) {
                    throw e;
                }
            }
        }
        if(skipVerification) {
            return layer;
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            //TODO: Make this accept options to better check the instantiation!
            layer.serialize(jc, stream,null);
        }
        catch(Exception e) {
            e.printStackTrace();
            throw new Exception(format("The serialization step of the communication layer fails for implementing class %s", layer.getClass().getCanonicalName()));
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                .version(HttpClient.Version.HTTP_1_1)
                .POST(HttpRequest.BodyPublishers.ofByteArray(stream.toByteArray()))
                .build();
                HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                if (resp.statusCode() == 200) {
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body());
                    try {
                        layer.deserialize(jc, inputStream);
                    }
                    catch(Exception e) {
                        System.err.printf("Caught exception printing response %s\n",new String(resp.body(), StandardCharsets.UTF_8));
                        throw e;
                    }
                    return layer;
                }
                else {
                    throw new Exception(format("The container returned response with code != 200\nResponse %s",resp.body().toString()));
                }
    }

    public boolean canAccept(DUUIPipelineComponent comp) {
        return comp.getDockerImageName()!=null;
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
        // String digest = _interface.getDigestFromImage(comp.getImageName());
        // comp.getPipelineComponent().__internalPinDockerImage(digest);
        String digest = comp.getImageName();
        System.out.printf("[DockerLocalDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(),digest);
        _active_components.put(uuid, comp);
        for (int i = 0; i < comp.getScale(); i++) {
            String containerid = _interface.run(digest, comp.usesGPU(), true, 9714,false);
            int port = _interface.extract_port_mapping(containerid);

            try {
                if (port == 0) {
                    throw new UnknownError("Could not read the container port!");
                }
                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = responsiveAfterTime("http://127.0.0.1:" + String.valueOf(port), jc, _container_timeout, _client,(msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                },_luaContext, skipVerification);
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), port);
                if (comp.isWebsocket()) {
                    String url = "http://127.0.0.1:" + String.valueOf(port);
                    _wsclient = new DUUIWebsocketAlt(
                            url.replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, 50);
                }
                else {
                    _wsclient = null;
                }
                comp.addInstance(new ComponentInstance(containerid, port, _wsclient));
                comp.setCommunicationLayer(layer);
            }
            catch(Exception e) {
                //_interface.stop_container(containerid);
                //throw e;
            }
        }
        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerLocalDriver][%s]: Maximum concurrency %d\n",uuid,component.getInstances().size());
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid,comp);
    }

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, CompressorException, CASException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = _active_components.get(uuid);
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
    public void shutdown() {
    }

    public void destroy(String uuid) {
        InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
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

        public ComponentInstance(String id, int port) {
            _container_id = id;
            _port = port;
        }

        public ComponentInstance(String id, int port, IDUUIConnectionHandler handler) {
            _container_id = id;
            _port = port;
            _handler = handler;
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

        public IDUUIConnectionHandler getHandler() {return _handler;}
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _image_name;
        private ConcurrentLinkedQueue<ComponentInstance> _instances;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private boolean _websocket;

        private String _reg_password;
        private String _reg_username;
        private String _uniqueComponentKey;

        private IDUUICommunicationLayer _layer;
        private Map<String,String> _parameters;
        private DUUIPipelineComponent _component;

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _layer;
        }

        public void setCommunicationLayer(IDUUICommunicationLayer layer) {
            _layer = layer;
        }



        public Triplet<IDUUIUrlAccessible,Long,Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _instances.poll();
            while(inst == null) {
                inst = _instances.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst,mutexStart,mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible access) {
            _instances.add((ComponentInstance) access);
        }

        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _image_name = comp.getDockerImageName();
            _parameters = comp.getParameters();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }
            _withImageFetching = comp.getDockerImageFetching(false);

            _uniqueComponentKey = "";


            _instances = new ConcurrentLinkedQueue<ComponentInstance>();

            _scale = comp.getScale(1);

            _gpu = comp.getDockerGPU(false);

            _keep_runnging_after_exit = comp.getDockerRunAfterExit(false);

            _reg_password = comp.getDockerAuthPassword();
            _reg_username = comp.getDockerAuthUsername();

            _websocket = comp.isWebsocket();
        }


        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {return _uniqueComponentKey;}
        public String getPassword() {return _reg_password;}

        public String getUsername() {return _reg_username;}

        public boolean getImageFetching() {return _withImageFetching;}

        public String getImageName() {
            return _image_name;
        }

        public int getScale() {
            return _scale;
        }

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public void addInstance(ComponentInstance inst) {
            _instances.add(inst);
        }

        public boolean usesGPU() {return _gpu;}

        public ConcurrentLinkedQueue<ComponentInstance> getInstances() {
            return _instances;
        }

        public Map<String,String> getParameters() {return _parameters;}

        public boolean isWebsocket() {
            return _websocket;
        }
    }

    public static class Component {
        private DUUIPipelineComponent _component;


        public Component withParameter(String key, String value) {
            _component.withParameter(key,value);
            return this;
        }

        public Component(String target) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
        }

        public Component withDescription(String description) {
            _component.withDescription(description);
            return this;
        }

        public Component withScale(int scale) {
            _component.withScale(scale);
            return this;
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

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIDockerDriver.class);
            return _component;
        }
    }
}
