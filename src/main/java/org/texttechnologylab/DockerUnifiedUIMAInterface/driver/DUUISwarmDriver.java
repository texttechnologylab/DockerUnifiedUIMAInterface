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
import org.texttechnologylab.DockerUnifiedUIMAInterface.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class DUUISwarmDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private OkHttpClient _client;

    private HashMap<String, DUUISwarmDriver.InstantiatedComponent> _active_components;
    private int _container_timeout;
    private DUUILuaContext _luaContext;


    public DUUISwarmDriver() throws IOException, SAXException, UIMAException {
        int timeout = 10;
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient.Builder()
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .writeTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .build();

        _container_timeout = 10000;

        _active_components = new HashMap<String, DUUISwarmDriver.InstantiatedComponent>();
    }

    public DUUISwarmDriver(int timeout) throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient.Builder()
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .writeTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .build();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _container_timeout = 10000;

        _active_components = new HashMap<String, DUUISwarmDriver.InstantiatedComponent>();
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    DUUISwarmDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    public boolean canAccept(IDUUIPipelineComponent comp) {
        return comp.getClass().getName().toString() == DUUISwarmDriver.Component.class.getName().toString();
    }

    public String instantiate(IDUUIPipelineComponent component) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }

        if(!_interface.isSwarmManagerNode()) {
            throw new InvalidParameterException("This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }

        JCas basic = JCasFactory.createJCas();
        basic.setDocumentLanguage("en");
        basic.setDocumentText("Hello World!");

        DUUISwarmDriver.InstantiatedComponent comp = new DUUISwarmDriver.InstantiatedComponent(component);

        if(comp.isBackedByLocalImage()) {
            System.out.printf("[DockerSwarmDriver] Attempting to push local image %s to remote image registry %s\n", comp.getLocalImageName(),comp.getImageName());
            if(comp.getUsername() != null && comp.getPassword() != null) {
                System.out.println("[DockerSwarmDriver] Using provided password and username to authentificate against the remote registry");
            }
            _interface.push_image(comp.getImageName(),comp.getLocalImageName(),comp.getUsername(),comp.getPassword());
        }
        System.out.printf("[DockerSwarmDriver] Assigned new pipeline component unique id %s\n", uuid);

            String serviceid = _interface.run_service(comp.getImageName(),comp.getScale());
            int port = _interface.extract_service_port_mapping(serviceid);

            if (port == 0) {
                throw new UnknownError("Could not read the service port!");
            }
            final String uuidCopy = uuid;
            IDUUICommunicationLayer layer = DUUILocalDriver.responsiveAfterTime("http://localhost:" + String.valueOf(port), basic, _container_timeout, _client, (msg) -> {
                System.out.printf("[DockerSwarmDriver][%s][%d Replicas] %s\n", uuidCopy, comp.getScale(),msg);
            },_luaContext);
            System.out.printf("[DockerSwarmDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(),comp.getImageName(), port);
            comp.initialise(serviceid,port);
            Thread.sleep(500);

        _active_components.put(uuid, comp);
        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        DUUISwarmDriver.InstantiatedComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerSwarmDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        Request request = new Request.Builder()
                .url(comp.getServiceUrl() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM)
                .get()
                .build();
        Response resp = _client.newCall(request).execute();
        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            File tmp = File.createTempFile("duui.composer","_type");
            FileWriter writer = new FileWriter(tmp);
            writer.write(body);
            writer.flush();
            writer.close();
            return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.getAbsolutePath());
        } else {
            System.out.printf("[DockerSwarmDriver][%s]: Endpoint did not provide typesystem, using default one...\n",uuid);
            return TypeSystemDescriptionFactory.createTypeSystemDescription();
        }
    }

    public JCas run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf) throws InterruptedException, IOException, SAXException, CompressorException {
        long mutexStart = System.nanoTime();
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        IDUUICommunicationLayer inst = comp.getInstances().poll();
        while(inst == null) {
            inst = comp.getInstances().poll();
        }
        long mutexEnd = System.nanoTime();
        long serializeStart = System.nanoTime();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        inst.serialize(aCas,out);
        String ok = out.toString();
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;

        RequestBody body = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));
        Request request = new Request.Builder()
                .url(comp.getServiceUrl()+ DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS)
                .post(body)
                .header("Content-Length", String.valueOf(ok.length()))
                .build();
        Response resp = _client.newCall(request).execute();

        if (resp.code() == 200) {
            ByteArrayInputStream st = new ByteArrayInputStream(resp.body().bytes());
            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;
            inst.deserialize(aCas,st);
            long deserializeEnd = System.nanoTime();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,mutexEnd-mutexStart,deserializeEnd-mutexStart,comp.getUniqueComponentKey());
            comp.returnCommunicationLayer(inst);
        } else {
            comp.returnCommunicationLayer(inst);
            throw new InvalidObjectException("Response code != 200, error");
        }
        return aCas;
    }

    public void destroy(String uuid) {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Swarm Driver");
        }
        if (!comp.getRunningAfterExit()) {
            System.out.printf("[DockerSwarmDriver] Stopping service %s...\n",comp.getServiceId());
            _interface.rm_service(comp.getServiceId());
        }
    }

    private static class InstantiatedComponent {
        private String _image_name;
        private String _service_id;
        private int _service_port;
        private boolean _keep_runnging_after_exit;
        private int _scale;
        private String _fromLocalImage;
        private ConcurrentLinkedQueue<IDUUICommunicationLayer> _communication;

        private String _reg_password;
        private String _reg_username;
        private String _uniqueComponentKey;

        public ConcurrentLinkedQueue<IDUUICommunicationLayer> getInstances() {
            return _communication;
        }

        public void addCommunicationLayer(IDUUICommunicationLayer layer) {
            for(int i = 0; i < _scale; i++) {
                _communication.add(layer.copy());
            }
        }

        public void returnCommunicationLayer(IDUUICommunicationLayer layer) {
            _communication.add(layer);
        }

        InstantiatedComponent(IDUUIPipelineComponent comp) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
            _image_name = comp.getOption("container");
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            _uniqueComponentKey = comp.getOption(DUUIComposer.COMPONENT_COMPONENT_UNIQUE_KEY);
            String scale = comp.getOption("scale");
            if (scale == null) {
                _scale = 1;
            } else {
                _scale = Integer.parseInt(scale);
            }
            _communication = new ConcurrentLinkedQueue<>();

            String with_running_after = comp.getOption("run_after_exit");
            if (with_running_after == null) {
                _keep_runnging_after_exit = false;
            } else {
                _keep_runnging_after_exit = with_running_after.equals("yes");
            }

            _fromLocalImage = comp.getOption("local_image");
            _reg_password = comp.getOption("reg_password");
            _reg_username = comp.getOption("reg_username");

            String classname = comp.getOption("mapper_class_name");
            _communication = null;
        }


        public String getUniqueComponentKey() {return _uniqueComponentKey;}
        public String getPassword() {return _reg_password;}

        public String getUsername() {return _reg_username;}

        public boolean isBackedByLocalImage() {
            return _fromLocalImage!=null;
        }

        public String getLocalImageName() {
            return _fromLocalImage;
        }


        public InstantiatedComponent initialise(String service_id, int container_port) {
            _service_id = service_id;
            _service_port = container_port;
            return this;
        }

        public String getServiceUrl() {
            return format("http://localhost:%d",_service_port);
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

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }
    }

    public static class Component extends IDUUIPipelineComponent {
        private String _target_name;
        private boolean _with_keep_runnging_after_exit;
        private int _with_scale;


        Component(String globalRegistryImageName) {
            setOption("container", globalRegistryImageName);
        }

        public Component withScale(int scale) {
            setOption("scale", String.valueOf(scale));
            return this;
        }

        public DUUISwarmDriver.Component withRegistryAuth(String username, String password) {
            setOption("reg_username",username);
            setOption("reg_password",password);
            return this;
        }

        public Component withFromLocalImage(String localImageName) {
            setOption("local_image",localImageName);
            return this;
        }


        public Component withRunningAfterDestroy(boolean run) {
            setOption("run_after_exit", (run) ? "yes" : "no");
            return this;
        }
    }
}
