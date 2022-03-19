import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;

public class DUUISwarmDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private OkHttpClient _client;

    private HashMap<String, DUUISwarmDriver.InstantiatedComponent> _active_components;

    private String _testCas;
    private String _testTypesystem;
    private int _container_timeout;
    private DUUICompressionHelper _helper;
    private DUUITypesystemCache _type_cache;


    DUUISwarmDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
        _type_cache = new DUUITypesystemCache();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        _container_timeout = 10000;

        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(_basic.getCas(), _basic.getTypeSystem(), arr);
        _testCas = arr.toString();

        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _testTypesystem = wr.getBuffer().toString();
        _active_components = new HashMap<String, DUUISwarmDriver.InstantiatedComponent>();
    }

    DUUISwarmDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    public boolean canAccept(IDUUIPipelineComponent comp) {
        return comp.getClass().getName().toString() == DUUISwarmDriver.Component.class.getName().toString();
    }

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }

        if(!_interface.isSwarmManagerNode()) {
            throw new InvalidParameterException("This node is not a Docker Swarm Manager, thus cannot create and schedule new services!");
        }

        JSONObject obj = new JSONObject();
        obj.put("cas", _testCas);
        obj.put("typesystem", _testTypesystem);
        String ok = obj.toString();

        RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));
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
            if (DUUILocalDriver.responsiveAfterTime("http://localhost:" + String.valueOf(port), bod, _container_timeout, _client)) {
                System.out.printf("[DockerSwarmDriver][%s][%d Replicas] Service for image %s is online (URL http://localhost:%d) and seems to understand DUUI V1 format!\n", uuid, comp.getScale(),comp.getImageName(), port);
                comp.initialise(serviceid,port);
                Thread.sleep(500);
            } else {
                _interface.rm_service(serviceid);
                throw new TimeoutException(format("The Service %s did not provide one succesful answer! Aborting...", serviceid));
            }

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

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, CompressorException {
        DUUISwarmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        comp.enter();

        String cas = aCas.getAsString();
        JCas fullcas = aCas.getAsJCas();
        _type_cache.update(fullcas.getTypeSystem(),_helper);
        JSONObject obj = new JSONObject();
        String cas_compressed = _helper.compress(cas);
        obj.put("cas", cas_compressed);
        obj.put("typesystem", _type_cache.getCompressedTypesystem());
        obj.put("typesystem_hash", _type_cache.getCompressedTypesystemHash());
        obj.put("cas_hash", cas_compressed.hashCode());
        obj.put("compression",_helper.getCompressionMethod());

        String ok = obj.toString();
        RequestBody bod = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));
        Request request = new Request.Builder()
                .url(comp.getServiceUrl()+ "/v1/process")
                .post(bod)
                .header("Content-Length", String.valueOf(ok.length()))
                .build();
        Response resp = _client.newCall(request).execute();
        comp.leave();

        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            JSONObject response = new JSONObject(body);
            if (response.has("cas") || response.has("error")) {
                aCas.updateStringBuffer(response.getString("cas"));
                return aCas;
            } else {
                System.out.println("The response is not in the expected format!");
                throw new InvalidObjectException("Response is not in the right format!");
            }
        } else {
            throw new InvalidObjectException("Response code != 200, error");
        }
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
        private AtomicInteger _maximum_concurrency;
        private String _fromLocalImage;

        private String _reg_password;
        private String _reg_username;

        public void enter() {
            while(true) {
                int before = _maximum_concurrency.get();
                while (before < 1) {
                    before = _maximum_concurrency.get();
                }
                int result = _maximum_concurrency.compareAndExchange(before,before-1);
                if(result==before)
                    return;
            }
        }

        public void leave() {
            _maximum_concurrency.addAndGet(1);
        }

        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _image_name = comp.getOption("container");
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

            String scale = comp.getOption("scale");
            if (scale == null) {
                _scale = 1;
            } else {
                _scale = Integer.parseInt(scale);
            }
            _maximum_concurrency = new AtomicInteger(_scale);

            String with_running_after = comp.getOption("run_after_exit");
            if (with_running_after == null) {
                _keep_runnging_after_exit = false;
            } else {
                _keep_runnging_after_exit = with_running_after.equals("yes");
            }

            _fromLocalImage = comp.getOption("local_image");
            _reg_password = comp.getOption("reg_password");
            _reg_username = comp.getOption("reg_username");
        }

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
