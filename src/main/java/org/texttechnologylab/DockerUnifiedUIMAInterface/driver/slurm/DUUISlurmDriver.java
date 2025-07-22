package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import io.vertx.core.cli.annotations.Hidden;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIFallbackCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIWebsocketAlt;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.slurmInDocker.SlurmRest;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

import javax.sound.sampled.Port;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static java.lang.String.format;


public class DUUISlurmDriver implements IDUUIDriverInterface {
    private SlurmRest _rest;
    private DUUIDockerInterface _docker_interface;
    private DUUILuaContext _luaContext;
    private DUUISlurmInterface _interface;

    private int _http_timeout_s = 10;
    private HashMap<String, DUUISlurmDriver.InstantiatedComponent> _active_components;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;
    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());


    public DUUISlurmDriver(SlurmRest rest) throws IOException, UIMAException, SAXException {
        _rest = rest;
        _interface = new DUUISlurmInterface(rest);
        _client = HttpClient.newHttpClient();

        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");
        //_http_timeout = 10000;
        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        // trigger for type checking
        desc.toXML(wr);
        _active_components = new HashMap<String, DUUISlurmDriver.InstantiatedComponent>();
        _luaContext = null;
    }

    /**
     * Creation of the communication layer based on the Driver
     *
     * @param url              localhost
     * @param jc               jcas
     * @param http_timeout_s   timeout with fastapi
     * @param client           communication wtih fastapi
     * @param context          lua
     * @param skipVerification true
     * @return
     * @throws Exception
     */
    public static IDUUICommunicationLayer responsiveAfterTime(String url, JCas jc, int http_timeout_s, HttpClient client, DUUILuaContext context, boolean skipVerification) throws Exception {
        long start = System.currentTimeMillis();
        // default http
        IDUUICommunicationLayer layer = new DUUIFallbackCommunicationLayer();  // Hier wird layer zum ersten mal erstellt.
        boolean fatal_error = false;

        // error counter
        int iError = 0;
        while (true) {
            HttpRequest request = null;
            try {// req
                // get lua script
                request = HttpRequest.newBuilder()
                        .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                        .version(HttpClient.Version.HTTP_1_1)
//                        .timeout(Duration.ofSeconds(10))
                        .timeout(Duration.ofSeconds(http_timeout_s))
                        .GET()
                        .build();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                HttpResponse<byte[]> resp = null;

                boolean connectionError = true;
                int iCount = 0;
                //max.10
                while (connectionError && iCount < 10) {

                    try {

                        // req lua
                        resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();

                        connectionError = false;
                    } catch (Exception e) {
                        System.out.println(e.getMessage() + "\t" + url);
                        if (e instanceof java.net.ConnectException) {
                            Thread.sleep(http_timeout_s);
                            iCount++;
                        } else if (e instanceof CompletionException) {
                            Thread.sleep(http_timeout_s);
                            iCount++;
                        }
                    }
                }
                // if success?
                if (resp.statusCode() == 200) {
                    // parse script as string
                    String body2 = new String(resp.body(), Charset.defaultCharset());
                    try {
                        //printfunc.operation("Component lua communication layer, loading...");
                        // new lua commu layer
                        IDUUICommunicationLayer lua_com = new DUUILuaCommunicationLayer(body2, "requester", context);
                        // replace
                        layer = lua_com;

                        //printfunc.operation("Component lua communication layer, loaded.");
                        break;
                    } catch (Exception e) {
                        fatal_error = true;
                        e.printStackTrace();
                        throw new Exception("Component provided a lua script which is not runnable.");
                    }
                    // fallback
                } else if (resp.statusCode() == 404) {
                    // printfunc.operation("Component provided no own communication layer implementation using fallback.");
                    break;
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if (timeElapsed > http_timeout_s) {
                    throw new TimeoutException(format("The Container did not provide one succesful answer in %d milliseconds", http_timeout_s));
                }

            } catch (Exception e) {

                if (fatal_error) {
                    throw e;
                } else {
                    Thread.sleep(2000l);
                    iError++;
                }

                if (iError > 10) {
                    throw e;
                }
            }
        }
        if (skipVerification) {
            return layer;
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {

            layer.serialize(jc, stream, null, "_InitialView");
        } catch (Exception e) {
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
                layer.deserialize(jc, inputStream, "_InitialView");
            } catch (Exception e) {
                System.err.printf("Caught exception printing response %s\n", new String(resp.body(), StandardCharsets.UTF_8));
                throw e;
            }
            return layer;
        } else {
            throw new Exception(format("The container returned response with code != 200\nResponse %s", resp.body().toString()));
        }
    }


    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }


    public boolean canAccept(DUUIPipelineComponent comp) {

       return comp.getSlurmScript()!= null || !comp.getSlurmRuntime().isEmpty();


    }


    /**
     *  The original code has changed a lot, so please check the interlinear comments.
     * @param component
     * @param jc
     * @param skipVerification
     * @param shutdown
     * @return
     * @throws InterruptedException
     * @throws URISyntaxException
     * @throws IOException
     */
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws InterruptedException, URISyntaxException, IOException {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }


        DUUISlurmDriver.InstantiatedComponent comp = new DUUISlurmDriver.InstantiatedComponent(component);

        System.out.printf("[SlurmDriver] Assigned new pipeline component unique id %s\n", uuid);

        _active_components.put(uuid, comp);

        for (int i = 0; i < comp.getScale(); i++) {
            if (shutdown.get()) {
                return null;
            }
            // If only one component is started, the user can specify a port on their own using withSlurmHostPort()
            // Otherwise it will be assigned by portmanager
            int hostPort = -1;
            String slurmHostPort = component.getSlurmHostPort();
            if (slurmHostPort == null || slurmHostPort.isEmpty()) {
                System.out.println("[SlurmDriver] Assigning a free port follows the FIFO-ALGORITHM");
                hostPort = PortManager.acquire();
            } else {
                System.out.println("[SlurmDriver] Assigning a port of your choice.");
                hostPort = Integer.parseInt(slurmHostPort);
            }
            //Users can generate scripts from the methods in SlurmUtil by entering a series of parameters,
            // or they can just enter a script in json format.
            System.out.println("[SlurmDriver] prepare to reserve port: " + hostPort);
            String job_id;
            if (component.getSlurmScript() != null) {
                String jobID = _interface.run_json(component, hostPort);
                job_id = jobID;
                System.out.println("[SlurmDriver] Submit successfully, Job ID: " + jobID);


            } else {
                //
                String jobID = _interface.run(component, hostPort);

                job_id = jobID;
                System.out.println("[SlurmDriver] submit " + component.getSlurmJobName() + " successfully, Job ID: " + jobID);
            }
            // Since submitting tasks in slurm is asynchronous,
            // the java code must be blocked to continue running, which prevents http error
            boolean ok = PortManager.waitUntilHttpReachable(
                    "http://localhost:" + hostPort + "/v1/communication_layer",
                    200,
                    null,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(2)
            );

            if (!ok) throw new IllegalStateException("timeout waiting for http ");

            // DUUISlurmInterface maintains a thread-safe hashmap<jobid,port>, see DUUISlurmInterface.class for details.
            String portFromMap = _interface.extractPort(job_id);
            if (!portFromMap.equals(Integer.toString(hostPort))) {
                throw new RuntimeException("port mismatch");
            }


            try {
                final int iCopy = i;
                final String uuidCopy = uuid;
                IDUUICommunicationLayer layer = responsiveAfterTime("http://127.0.0.1" + ":" + hostPort, jc, _http_timeout_s, _client, _luaContext, skipVerification);
                if (component.getScale()==null)
                {
                    System.out.println("[SlurmDriver] " + component.getSlurmSIFImageName() + "[" + Integer.toString(iCopy + 1) + "/" + "1" + "]" + "is online and seems to understand DUUI V1 format!");

                }

                System.out.println("[SlurmDriver] " + component.getSlurmSIFImageName() + "[" + Integer.toString(iCopy + 1) + "/" + component.getScale() + "]" + "is online and seems to understand DUUI V1 format!");
                if (comp.isWebsocket()) {
                    String url = "ws://127.0.0.1:" + hostPort;
                    _wsclient = new DUUIWebsocketAlt(
                            url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, comp.getWebsocketElements());
                } else {
                    _wsclient = null;
                }

                comp.addInstance(new DUUISlurmDriver.ComponentInstance(job_id, hostPort, layer, _wsclient));
            } catch (Exception e) {

            }

        }
        return shutdown.get() ? null : uuid;
    }


    /**
     * Show the maximum parallelism
     *
     * @param uuid
     */
    @Override
    public void printConcurrencyGraph(String uuid) {
        DUUISlurmDriver.InstantiatedComponent component = _active_components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[DockerLocalDriver][%s]: Maximum concurrency %d\n", uuid, component.getInstances().size());
    }


    /**
     * Return the TypeSystem used by the given Component
     *
     * @param uuid
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws SAXException
     * @throws CompressorException
     * @throws ResourceInitializationException
     */
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUISlurmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }


    /**
     * init reader component
     *
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
    }
//-------------------------------------------------------------------------------------------------------------


    public static class ComponentInstance implements IDUUIUrlAccessible {
        private String _job_ID;
        private int _hostPort;
        private IDUUIConnectionHandler _handler;
        private IDUUICommunicationLayer _communicationLayer;

        public ComponentInstance(String id, int hostPort, IDUUICommunicationLayer communicationLayer) {
            _job_ID = id;
            _hostPort = hostPort;
            _communicationLayer = communicationLayer;
        }

        public ComponentInstance(String id, int hostPort, IDUUICommunicationLayer communicationLayer, IDUUIConnectionHandler handler) {
            _job_ID = id;
            _hostPort = hostPort;

            _communicationLayer = communicationLayer;
            _handler = handler;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }


        public String get_job_ID() {
            return _job_ID;
        }

        public int get_hostPort() {
            return _hostPort;
        }


        public IDUUIConnectionHandler get_handler() {
            return _handler;
        }

        public IDUUICommunicationLayer get_communicationLayer() {
            return _communicationLayer;
        }

        public String generateURL() {
            return format("http://127.0.0.1" + ":%d", _hostPort);
        }

        public String generateURL(String localhost) {
            return format("http://localhost" + ":%d", _hostPort);
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    /**
     * Execute a component in the driver
     *
     * @param uuid
     * @param aCas
     * @param perf
     * @param composer
     * @throws CASException
     */
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException {
        long mutexStart = System.nanoTime();
        DUUISlurmDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (comp.isWebsocket()) {
            IDUUIInstantiatedPipelineComponent.process_handler(aCas, comp, perf);
        } else {
            IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);
        }
    }


    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _image_name;
        private ConcurrentLinkedQueue<DUUISlurmDriver.ComponentInstance> _instances;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private boolean _websocket;
        private int _ws_elements;
        private String _uniqueComponentKey;
        private Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;

        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            DUUISlurmDriver.ComponentInstance inst = _instances.poll();
            while (inst == null) {
                inst = _instances.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        public void addComponent(IDUUIUrlAccessible access) {
            _instances.add((DUUISlurmDriver.ComponentInstance) access);
        }

        InstantiatedComponent(DUUIPipelineComponent comp) {
            _component = comp;
            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();

            _withImageFetching = comp.getDockerImageFetching(false);

            _uniqueComponentKey = "";


            _instances = new ConcurrentLinkedQueue<DUUISlurmDriver.ComponentInstance>();

            _scale = comp.getScale(1);

            _gpu = comp.getDockerGPU(false);

            _keep_runnging_after_exit = comp.getSlurmRunAfterExit(false);

            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

        public boolean getImageFetching() {
            return _withImageFetching;
        }

        public String getImageName() {
            return _image_name;
        }

        public int getScale() {
            return _scale;
        }

        public boolean getRunningAfterExit() {
            return _keep_runnging_after_exit;
        }

        public void addInstance(DUUISlurmDriver.ComponentInstance inst) {
            _instances.add(inst);
        }

        public boolean usesGPU() {
            return (Integer.parseInt(_component.getSlurmGPU()) > 0) ? true : false;
        }

        public ConcurrentLinkedQueue<DUUISlurmDriver.ComponentInstance> getInstances() {
            return _instances;
        }

        public Map<String, String> getParameters() {
            return _parameters;
        }

        public String getSourceView() {
            return _sourceView;
        }

        public String getTargetView() {
            return _targetView;
        }

        public boolean isWebsocket() {
            return _websocket;
        }

        public int getWebsocketElements() {
            return _ws_elements;
        }
    }


    public static class Component {
        private DUUIPipelineComponent _component;


        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
            _component.withDriver(DUUISlurmDriver.class);
        }

        public DUUIPipelineComponent build() {
            return _component;
        }

    }

    @Override
    public void shutdown() throws IOException, InterruptedException{
        String token = _rest.generateRootToken("slurmctld");
        System.out.println("shutdown hook clean all jobs");
        HashMap<String, String> jobIDPortMap = _interface.get_jobID_PortMap();
        jobIDPortMap.keySet().forEach(jobID -> {
            try {
                _rest.cancelJob(token, jobID);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }


    /**
     * Because it is a self-maintained port table,
     * it allows the port to be released to another component for continued use after the end of each type of component.
     * See PortManager.class for details
     * @param uuid
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean destroy(String uuid) throws IOException, InterruptedException {

        String token = _rest.generateRootToken("slurmctld");
        DUUISlurmDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        boolean flag = true;
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (DUUISlurmDriver.ComponentInstance inst : comp.getInstances()) {
                String jobId = inst.get_job_ID();
                boolean status = _rest.cancelJob(token, jobId);
                PortManager.release(inst.get_hostPort());
                _interface.get_jobID_PortMap().remove(jobId);
                counter += 1;
                flag = flag && status;

            }
        }
        return flag;
    }
}


