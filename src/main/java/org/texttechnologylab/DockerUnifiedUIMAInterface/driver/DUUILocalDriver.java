package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.texttechnologylab.DockerUnifiedUIMAInterface.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
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
    private OkHttpClient _client;

    private HashMap<String, InstantiatedComponent> _active_components;
    private int _container_timeout;
    private DUUILuaContext _luaContext;

    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    public DUUILocalDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient();

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
        _client = new OkHttpClient.Builder()
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .writeTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .build();

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

    public static IDUUICommunicationLayer responsiveAfterTime(String url, JCas jc, int timeout_ms, OkHttpClient client, ResponsiveMessageCallback printfunc, DUUILuaContext context) throws Exception {
        long start = System.currentTimeMillis();
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();
        boolean fatal_error = false;
        while(true) {
            Request request = new Request.Builder()
                    .url(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER)
                    .get()
                    .build();
            try {
                Response resp = client.newCall(request).execute();
                if (resp.code() == 200) {
                    String body2 = new String(resp.body().bytes(), Charset.defaultCharset());
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
                } else if (resp.code() == 404) {
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
            layer.serialize(jc, stream);
        }
        catch(Exception e) {
            e.printStackTrace();
            throw new Exception(format("The serialization step of the communication layer fails for implementing class %s", layer.getClass().getCanonicalName()));
        }

        RequestBody body = RequestBody.create(stream.toByteArray());

        Request request = new Request.Builder()
                .url(url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS)
                .post(body)
                .build();
                Response resp = client.newCall(request).execute();
                if (resp.code() == 200) {
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body().bytes());
                    layer.deserialize(jc,inputStream);
                    return layer;
                }
                else {
                    throw new Exception(format("The container returned response with code != 200\nResponse %s",resp.body().bytes().toString()));
                }
    }

    public boolean canAccept(IDUUIPipelineComponent comp) {
        return comp.getClass().getName().toString() == Component.class.getName().toString();
    }

    public String instantiate(IDUUIPipelineComponent component) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component);
        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");


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
                IDUUICommunicationLayer layer = responsiveAfterTime("http://127.0.0.1:" + String.valueOf(port), _basic, _container_timeout, _client, (msg) -> {
                    System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                },_luaContext);
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), port);
                comp.addInstance(new ComponentInstance(containerid, port, layer));
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
        ComponentInstance inst = comp.getInstances().poll();
        while(inst == null) {
            inst = comp.getInstances().poll();
        }
        Request request = new Request.Builder()
                .url(inst.getContainerUrl() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM)
                .get()
                .build();
        Response resp = _client.newCall(request).execute();
        comp.addInstance(inst);
        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            File tmp = File.createTempFile("duui.composer","_type");
            FileWriter writer = new FileWriter(tmp);
            writer.write(body);
            writer.flush();
            writer.close();
            return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.getAbsolutePath());
        } else {
            System.out.printf("[DockerLocalDriver][%s]: Endpoint did not provide typesystem, using default one...\n",uuid);
            return TypeSystemDescriptionFactory.createTypeSystemDescription();
        }
    }

    public JCas run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, CompressorException {
        long mutexStart = System.nanoTime();
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        ComponentInstance inst = comp.getInstances().poll();
        while(inst == null) {
            inst = comp.getInstances().poll();
        }
        long mutexEnd = System.nanoTime();


        long serializeStart = System.nanoTime();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        inst.getCommunicationLayer().serialize(aCas,outputStream);
        long serializeEnd = System.nanoTime();

        byte []ok = outputStream.toByteArray();
        long annotatorStart = serializeEnd;
        RequestBody bod = RequestBody.create(ok);
        Request request = new Request.Builder()
                .url(inst.getContainerUrl() + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS)
                .post(bod)
                .header("Content-Length", String.valueOf(ok.length))
                .build();
        Response resp = _client.newCall(request).execute();

        if (resp.code() == 200) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body().bytes());
            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;
            inst.getCommunicationLayer().deserialize(aCas,inputStream);
            long deserializeEnd = System.nanoTime();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,mutexEnd-mutexStart,deserializeEnd-mutexStart,comp.getUniqueComponentKey());
            comp.addInstance(inst);
        } else {
            comp.addInstance(inst);
            throw new InvalidObjectException("Response code != 200, error");
        }
        return aCas;
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

    public static class ComponentInstance {
        private String _container_id;
        private int _port;
        private IDUUICommunicationLayer _communication;

        public ComponentInstance(String id, int port, IDUUICommunicationLayer layer) {
            _container_id = id;
            _port = port;
            _communication = layer;
        }

        String getContainerId() {
            return _container_id;
        }

        int getContainerPort() {
            return _port;
        }

        IDUUICommunicationLayer getCommunicationLayer() {
            return _communication;
        }

        String getContainerUrl() {
            return format("http://127.0.0.1:%d", _port);
        }
    }

    static class InstantiatedComponent {
        private String _image_name;
        private boolean _local;
        private ConcurrentLinkedQueue<ComponentInstance> _instances;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;

        private String _reg_password;
        private String _reg_username;
        private String _uniqueComponentKey;

        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _image_name = comp.getOption("container");
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
