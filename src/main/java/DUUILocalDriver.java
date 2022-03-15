import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static java.lang.String.format;

public class DUUILocalDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private OkHttpClient _client;

    private HashMap<String, InstantiatedComponent> _active_components;

    private String _testCas;
    private String _testTypesystem;
    private int _container_timeout;

    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    DUUILocalDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient();

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
        _active_components = new HashMap<String, InstantiatedComponent>();
    }

    DUUILocalDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    public static boolean responsiveAfterTime(String url, RequestBody body, int timeout_ms, OkHttpClient client) {
        long start = System.currentTimeMillis();
        while (true) {
            Request request = new Request.Builder()
                    .url(url + "/v1/process")
                    .post(body)
                    .build();
            try {
                Response resp = client.newCall(request).execute();
                if (resp.code() == 200) {
                    String body2 = new String(resp.body().bytes(), Charset.defaultCharset());
                    JSONObject response = new JSONObject(body2);
                    if (response.has("cas") || response.has("error")) {
                        break;
                    } else {
                        LOGGER.info("The response is not in the expected format!");
                    }
                }

            } catch (Exception e) {
                //e.printStackTrace();
            }
            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            if (timeElapsed > timeout_ms) {
                return false;
            }
        }
        return true;
    }

    public boolean canAccept(IDUUIPipelineComponent comp) {
        return comp.getClass().getName().toString() == Component.class.getName().toString();
    }

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }

        JSONObject obj = new JSONObject();
        obj.put("cas", _testCas);
        obj.put("typesystem", _testTypesystem);
        String ok = obj.toString();

        RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));
        InstantiatedComponent comp = new InstantiatedComponent(component);


        if (!comp.isLocal()) {
            _interface.pullImage(comp.getImageName());
            System.out.printf("[DockerLocalDriver] Pulled image with id %s\n",comp.getImageName());
        }
        else {
            if(!_interface.hasLocalImage(comp.getImageName())) {
                throw new InvalidParameterException(format("Could not find local docker image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?",comp.getImageName()));
            }
        }
        System.out.printf("[DockerLocalDriver] Assigned new pipeline component unique id %s\n", uuid);
        for (int i = 0; i < comp.getScale(); i++) {
            String containerid = _interface.run(comp.getImageName(), false, true);
            int port = _interface.extract_port_mapping(containerid);

            if (port == 0) {
                throw new UnknownError("Could not read the container port!");
            }
            if (responsiveAfterTime("http://127.0.0.1:" + String.valueOf(port), bod, _container_timeout, _client)) {
                System.out.printf("[DockerLocalDriver][%s][Docker Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), port);
                comp.addInstance(new ComponentInstance(containerid, port));
            } else {
                _interface.stop_container(containerid);
                throw new TimeoutException(format("The Container %s did not provide one succesful answer! Aborting...", containerid));
            }
        }
        _active_components.put(uuid, comp);
        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerLocalDriver][%s]: Maximum concurrency %d\n",uuid,component.getInstances().size());
    }

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException {
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        ComponentInstance inst = comp.getInstances().poll();
        while(inst == null) {
            inst = comp.getInstances().poll();
        }

        String cas = aCas.getAsString();

        JSONObject obj = new JSONObject();
        obj.put("cas", cas);
        obj.put("typesystem", "");
        String ok = obj.toString();

        RequestBody bod = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));
        Request request = new Request.Builder()
                .url(inst.getContainerUrl() + "/v1/process")
                .post(bod)
                .header("Content-Length", String.valueOf(ok.length()))
                .build();
        Response resp = _client.newCall(request).execute();
        comp.addInstance(inst);
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

    static class ComponentInstance {
        private String _container_id;
        private int _port;

        ComponentInstance(String id, int port) {
            _container_id = id;
            _port = port;
        }

        String getContainerId() {
            return _container_id;
        }

        int getContainerPort() {
            return _port;
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

        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _image_name = comp.getOption("container");
            if (_image_name == null) {
                throw new InvalidParameterException("The image name was not set! This is mandatory for the DockerLocalDriver Class.");
            }

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
        }

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


        Component(String target) {
            setOption("container", target);
            setOption("local", "yes");
        }

        public Component withScale(int scale) {
            setOption("scale", String.valueOf(scale));
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