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
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static java.lang.String.format;

public class DUUILocalDriver implements IDUUIDriverInterface {
    private DUUIDockerInterface _interface;
    private OkHttpClient _client;

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
        LOGGER.info("Generated test cas "+_testCas);

        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _testTypesystem = wr.getBuffer().toString();
        LOGGER.info("Generated test cas typesystem "+_testTypesystem);
    }

    DUUILocalDriver withTimeout(int container_timeout_ms) {
        _container_timeout = container_timeout_ms;
        return this;
    }

    private boolean responsiveAfterTime(String url, RequestBody body, int timeout_ms) {
        long start = System.currentTimeMillis();
        while(true) {
            Request request = new Request.Builder()
                    .url(url+"/v1/process")
                    .post(body)
                    .build();
            try {
                Response resp = _client.newCall(request).execute();
                if (resp.code()==200) {
                    String body2 = new String(resp.body().bytes(), Charset.defaultCharset());
                    JSONObject response = new JSONObject(body2);
                    if(response.has("cas") || response.has("error")) {
                        break;
                    }
                    else {
                        LOGGER.info("The response is not in the expected format!");
                    }
                }

            }
            catch(Exception e) {
                //e.printStackTrace();
            }
            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            if(timeElapsed>timeout_ms) {
                return false;
            }
        }
        return true;
    }

    public boolean canAccept(IDUUIPipelineComponent comp) {
        return comp.getClass().getName().toString() == Component.class.getName().toString();
    }

    public boolean instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException {
        JSONObject obj = new JSONObject();
        obj.put("cas",_testCas);
        obj.put("typesystem",_testTypesystem);
        String ok = obj.toString();

        RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));
        Component comp = new Component(component);
        if (comp.isRunning()) {
            return true;
        }
        if (!comp.isLocal()) {
            _interface.pullImage(comp.getTarget());
        }
        System.out.println(comp.getTarget());
        String containerid = _interface.run(comp.getTarget(), false, false);
        int port = _interface.extract_port_mapping(containerid);

        LOGGER.info(format("Got container %s with port %d\n", containerid, port));
        if (port == 0) {
            throw new UnknownError("Could not read the container port!");
        }
        if (responsiveAfterTime("http://127.0.0.1:" + String.valueOf(port), bod, _container_timeout)) {
            System.out.printf("Container for image %s is online and seems to understand DUUI V1 format!\n", comp.getTarget());
            comp._internalSetRunning(containerid, port);
        } else {
            _interface.stop_container(containerid);
            throw new TimeoutException(format("The Container %s did not provide one succesful answer! Aborting...", containerid));
        }
        return true;
    }


    public void run(IDUUIPipelineComponent comp, JCas aCas) throws InterruptedException, IOException, SAXException {
        Component component = new Component(comp);

            if(!component.isRunning()) {
                System.out.println("Skipping stopped container");
                return;
            }
            ByteArrayOutputStream arr = new ByteArrayOutputStream();
            XmiCasSerializer.serialize(aCas.getCas(), aCas.getTypeSystem(), arr);
            String cas = arr.toString();

            TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(aCas.getTypeSystem());
            StringWriter wr = new StringWriter();
            desc.toXML(wr);
            String typesystem = wr.getBuffer().toString();

            JSONObject obj = new JSONObject();
            obj.put("cas", cas);
            obj.put("typesystem", typesystem);
            String ok = obj.toString();

            RequestBody bod = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));

                Request request = new Request.Builder()
                        .url("http://localhost:" + String.valueOf(component.getRunningContainerPort()) + "/v1/process")
                        .post(bod)
                        .header("Content-Length",String.valueOf(ok.length()))
                        .build();
                    Response resp = _client.newCall(request).execute();
                    if (resp.code() == 200) {
                        String body = new String(resp.body().bytes(), Charset.defaultCharset());
                        JSONObject response = new JSONObject(body);
                        if (response.has("cas") || response.has("error")) {
                            aCas.reset();
                            XmiCasDeserializer.deserialize(new ByteArrayInputStream(response.getString("cas").getBytes(StandardCharsets.UTF_8)), aCas.getCas());
                        } else {
                            System.out.println("The response is not in the expected format!");
                            throw new InvalidObjectException("Response is not in the right format!");
                        }
                    }
                    else {
                        throw new InvalidObjectException("Response code != 200, error");
                    }
    }

    public void destroy(IDUUIPipelineComponent component) {
        Component comp = new Component(component);
        if (!comp.isRunning()) {
            return;
        }
        _interface.stop_container(comp.getRunningContainerId());
        comp._internalReset();
    }

    public static class Component extends IDUUIPipelineComponent {
        private String _target_name;
        private boolean _is_local;
        private boolean _with_gpu;
        private boolean _with_keep_runnging_after_exit;
        private int _with_scale;

        private String _container_id;
        private int _container_port;


        private String _testCas;
        private String _testTypesystem;
        Component(String target, boolean local) {
            setOption("container",target);
            setOption("local",(local)?"yes":"no");
        }

        Component(IDUUIPipelineComponent other) {
            super(other);
            if(!other.getClass().getCanonicalName().toString().equals(Component.class.getCanonicalName().toString())) {
                throw new InvalidParameterException(format("Trying to initialize DockerDUUIPipelineComponent from %s",other.getClass().getCanonicalName().toString()));
            }
        }

        public Component _internalSetRunning(String containerId, int port) {
            setOption("container_id",containerId);
            setOption("port",String.valueOf(port));
            return this;
        }

        public Component _internalReset() {
            removeOption("container_id");
            removeOption("port");
            return this;
        }

        public String getRunningContainerId() {
            return getOption("container_id");
        }

        public int getRunningContainerPort() {
            return Integer.valueOf(getOption("port"));
        }

        public boolean isRunning() {
            return hasOption("container_id")&&hasOption("port");
        }

        public String getTarget() {
            return getOption("container");
        }

        public boolean isRemote() {
            return getOption("local")=="no";
        }

        public boolean isLocal() {
            return getOption("local")=="yes";
        }

        public int getScale() {
            return Integer.parseInt(getOption("scale"));
        }
    }
}