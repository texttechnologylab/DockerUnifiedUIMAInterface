package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;


import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static java.lang.String.format;

interface ResponsiveMessageCallback {
    public void operation(String message);
}

public class DUUILocalDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private HttpClient _client;


    private HashMap<String, InstantiatedComponent> _active_components;
    private int _container_timeout;
    private DUUILuaContext _luaContext;

    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUILocalDriver() throws IOException, UIMAException, SAXException {
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

    public DUUILocalDriver(int timeout) throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeout)).build();

        _container_timeout = 10000;

        _active_components = new HashMap<String, InstantiatedComponent>();
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    public DUUILocalDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    public static IDUUICommunicationLayer responsiveAfterTime(String url, JCas jc, int timeout_ms, HttpClient client, ResponsiveMessageCallback printfunc, DUUILuaContext context) throws Exception {
        long start = System.currentTimeMillis();
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();
        boolean fatal_error = false;
        while(true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
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
                .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                .POST(HttpRequest.BodyPublishers.ofByteArray(stream.toByteArray()))
                .build();
                HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
                if (resp.statusCode() == 200) {
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body());
                    layer.deserialize(jc,inputStream);
                    return layer;
                }
                else {
                    throw new Exception(format("The container returned response with code != 200\nResponse %s",resp.body().toString()));
                }
    }

    public boolean canAccept(IDUUIPipelineComponent comp) {
        return comp.getClass().getName().toString() == Component.class.getName().toString();
    }

    public String instantiate(IDUUIPipelineComponent component, JCas jc) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component);

        if (!comp.isLocal()) {
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
        _active_components.put(uuid, comp);
        for (int i = 0; i < comp.getScale(); i++) {
            String containerid = _interface.run(comp.getImageName(), false, true);
            int port = _interface.extract_port_mapping(containerid);

            try {
                if (port == 0) {
                    throw new UnknownError("Could not read the container port!");
                }
                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = responsiveAfterTime("http://127.0.0.1:" + String.valueOf(port), jc, _container_timeout, _client, (msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                },_luaContext);
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), port);
                comp.addInstance(new ComponentInstance(containerid, port));
                comp.setCommunicationLayer(layer);
            }
            catch(Exception e) {
                _interface.stop_container(containerid);
                throw e;
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

    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, CompressorException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        IDUUIInstantiatedPipelineComponent.process(aCas,comp,perf);
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

        public ComponentInstance(String id, int port) {
            _container_id = id;
            _port = port;
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
    }

    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _image_name;
        private boolean _local;
        private ConcurrentLinkedQueue<ComponentInstance> _instances;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;

        private String _reg_password;
        private String _reg_username;
        private String _uniqueComponentKey;
        private IDUUICommunicationLayer _layer;
        private Map<String,String> _parameters;

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

        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _image_name = comp.getOption("container");
            _parameters = comp.getParameters();
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _uniqueComponentKey = comp.getOption(DUUIComposer.COMPONENT_COMPONENT_UNIQUE_KEY);

            String local = comp.getOption("local");
            if (local != null && local.equals("yes")) {
                _local = true;
            } else {
                _local = false;
            }
            _instances = new ConcurrentLinkedQueue<ComponentInstance>();

            String scale = comp.getOption("scale");
            if (scale == null) {
                _scale = 1;
            } else {
                _scale = Integer.parseInt(scale);
            }

            String gpu = comp.getOption("gpu");
            if (gpu == null) {
                _gpu = false;
            } else {
                _gpu = gpu.equals("yes");
            }

            String with_running_after = comp.getOption("run_after_exit");
            if (with_running_after == null) {
                _keep_runnging_after_exit = false;
            } else {
                _keep_runnging_after_exit = with_running_after.equals("yes");
            }

            _reg_password = comp.getOption("reg_password");
            _reg_username = comp.getOption("reg_username");
        }

        public String getUniqueComponentKey() {return _uniqueComponentKey;}
        public String getPassword() {return _reg_password;}

        public String getUsername() {return _reg_username;}


        public String getImageName() {
            return _image_name;
        }

        public boolean isLocal() {
            return _local;
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

        public ConcurrentLinkedQueue<ComponentInstance> getInstances() {
            return _instances;
        }

        public Map<String,String> getParameters() {return _parameters;}
    }

    public static class Component extends IDUUIPipelineComponent {
        private String _target_name;
        private boolean _is_local;
        private boolean _with_gpu;
        private boolean _with_keep_runnging_after_exit;
        private int _with_scale;


        public Component(String target) {
            setOption("container", target);
            setOption("local", "yes");
        }

        public Component withScale(int scale) {
            setOption("scale", String.valueOf(scale));
            return this;
        }

        public Component withRegistryAuth(String username, String password) {
            setOption("reg_username",username);
            setOption("reg_password",password);
            return this;
        }

        public Component withImageFetching() {
            setOption("local", "no");
            return this;
        }

        public Component withGPU(boolean gpu) {
            setOption("gpu", (gpu) ? "yes" : "no");
            return this;
        }

        public Component withRunningAfterDestroy(boolean run) {
            setOption("run_after_exit", (run) ? "yes" : "no");
            return this;
        }
    }
}
