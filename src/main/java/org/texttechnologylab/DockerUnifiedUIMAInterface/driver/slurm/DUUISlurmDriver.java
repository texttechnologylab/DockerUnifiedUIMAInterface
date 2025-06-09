package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

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
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
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
import static org.bouncycastle.oer.its.ieee1609dot2.SignerIdentifier.digest;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.PortManager.waitUntilReachable;

public class DUUISlurmDriver implements IDUUIDriverInterface {
    // Man kann den Docker-Client dazu verwenden, ein Image herunterzuladen (per Pull).
    private DUUIDockerInterface _docker_interface;
    private DUUILuaContext _luaContext;
    private DUUISlurmInterface _interface;

    private int _http_timeout_s = 10;
    private HashMap<String, DUUISlurmDriver.InstantiatedComponent> _active_components;
    private HttpClient _client;
    private IDUUIConnectionHandler _wsclient;
    private final static Logger LOGGER = Logger.getLogger(DUUIComposer.class.getName());

    //A
    public DUUISlurmDriver() throws IOException, UIMAException, SAXException {
        _interface = new DUUISlurmInterface();
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

    //B

    /**
     * Creation of the communication layer based on the Driver
     *
     * @param url              localhost
     * @param jc               jcas
     * @param http_timeout_s       timeout with fastapi
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

        // cas seri
        try {
            //TODO: Make this accept options to better check the instantiation!
            layer.serialize(jc, stream, null, "_InitialView");
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(format("The serialization step of the communication layer fails for implementing class %s", layer.getClass().getCanonicalName()));
        }
// http req
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                .version(HttpClient.Version.HTTP_1_1)
                .POST(HttpRequest.BodyPublishers.ofByteArray(stream.toByteArray()))
                .build();
        // resp
        HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();

        if (resp.statusCode() == 200) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(resp.body());
            try {
                layer.deserialize(jc, inputStream, "_InitialView");
            } catch (Exception e) {
                System.err.printf("Caught exception printing response %s\n", new String(resp.body(), StandardCharsets.UTF_8));
                throw e;
            }
            // use for shallow copy
            return layer;
        } else {
            throw new Exception(format("The container returned response with code != 200\nResponse %s", resp.body().toString()));
        }
    }


    //C
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    //-------------------------------------------------------------------------------------------------------------
    //D no use
//-------------------------------------------------------------------------------------------------------------
    //E
    public boolean canAccept(DUUIPipelineComponent comp) {
        return comp.getSlurmSIFImageName() != null;
    }
//-------------------------------------------------------------------------------------------------------------
    //F

    /**
     * Instantiate the component
     *
     * @param component
     * @param jc
     * @param skipVerification
     * @return
     * @throws Exception
     */
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws InterruptedException, URISyntaxException, IOException {
        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }


        DUUISlurmDriver.InstantiatedComponent comp = new DUUISlurmDriver.InstantiatedComponent(component);

        // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.



//        if (!(component.getSlurmSIFRepoLocation() == null)) {
//            // mean local no sif file
//            //_docker_interface.pullImage(component.getDockerImageName());
//            sif iamge name in dockerimagename
//            SlurmUtils.pullSifImagefromRemoteDockerRepo(component.getSlurmSIFImageName(), component.getDockerImageName());
//        }
//        //
//        else

        // etwas neues
        //
        if (!(component.getSlurmSIFDiskLocation() == null)) {
            //SlurmUtils.pullSifImagefromLocalDockerRepo(component.getSlurmSIFImageName(),component.getSlurmSIFImageName());


            System.out.println("sif file existed");
        } else {
            throw new RuntimeException("SIF File conflict detected, either local disk location or remote repo url ");
        }
        System.out.printf("[SlurmDriver] Assigned new pipeline component unique id %s\n", uuid);
//        String digest = _interface.getDigestFromImage(comp.getImageName());

        //comp.getPipelineComponent().__internalPinDockerImage(comp.getImageName(), digest);
        //System.out.printf("[DockerLocalDriver] Transformed image %s to pinnable image name %s\n", comp.getImageName(), comp.getPipelineComponent().getDockerImageName());

        _active_components.put(uuid, comp);

        for (int i = 0; i < comp.getScale(); i++) {
            if (shutdown.get()) {
                return null;
            }
//
            int hostPort = PortManager.acquire();
            System.out.println("prepare to reserve port " + hostPort);
            // dient als placeholder
            try (PortManager.PortReservation r = new PortManager.PortReservation(hostPort)) {
                //
                //
                // placeholder muss vor dem Lauf eines Komponent freilassen werden, sonst entsteht ein Konflikt
                r.close();
                String jobID = _interface.run(component, hostPort);

                String port = _interface.extractPort(jobID);  //
                if (!port.equals(Integer.toString(hostPort))) {
                    throw new RuntimeException("port mismatch");
                }



                //wait test if ok pass
                waitUntilReachable("127.0.0.1", hostPort, Duration.ofMinutes(1));

                try {
                    final int iCopy = i;
                    final String uuidCopy = uuid;
                    IDUUICommunicationLayer layer = responsiveAfterTime("http://127.0.0.1" + ":" + port, jc, _http_timeout_s, _client, _luaContext, skipVerification);

                    if (comp.isWebsocket()) {
                        String url = "ws://127.0.0.1:" + port;
                        _wsclient = new DUUIWebsocketAlt(
                                url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET, comp.getWebsocketElements());
                    } else {
                        _wsclient = null;
                    }
                    /**
                     * @see
                     * @edited
                     * Dawit Terefe
                     *
                     * Saves websocket client in ComponentInstance for
                     * retrieval in process_handler-function.
                     */
                    comp.addInstance(new DUUISlurmDriver.ComponentInstance(jobID, hostPort, layer, _wsclient));
                } catch (Exception e) {
                    //_interface.stop_container(containerid);
                    //throw e;
                }
            }
        }
        return shutdown.get() ? null : uuid;
    }

    @Override
    public void printConcurrencyGraph(String uuid) {

    }


////G no use
//    /**
//     * Show the maximum parallelism
//     *
//     * @param uuid
//     */
//    public void printConcurrencyGraph(String uuid) {
//        DUUISlurmDriver.InstantiatedComponent component = _active_components.get(uuid);
//        if (component == null) {
//            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
//        }
//        System.out.printf("[DockerLocalDriver][%s]: Maximum concurrency %d\n", uuid, component.getInstances().size());
//    }

//-----------------------------------------------------------------------------------------------------------------------
//H  uuid 1--------------------n same type component

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

    @Override
    public int initReaderComponent(String uuid, Path filePath) throws Exception {
        return 0;
    }


    /// /I  what is this?
//    /**
//     * init reader component
//     * @param uuid
//     * @param filePath
//     * @return
//     */
//    @Override
//    public int initReaderComponent(String uuid, Path filePath) {
//        InstantiatedComponent comp = _active_components.get(uuid);
//        if (comp == null) {
//            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
//        }
//        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
//    }
//-------------------------------------------------------------------------------------------------------------
//J over write service logic


    //K no use

//L close  no use

    //M
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

//        public int get_fastAPIPort() {
//            return _fastAPIPort;
//        }

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

    //------------------------------------------------------------------------------------
//J

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

    @Override
    public boolean destroy(String uuid) {
        return false;
    }

    //N
    static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private String _image_name;
        private ConcurrentLinkedQueue<DUUISlurmDriver.ComponentInstance> _instances;
        private boolean _gpu;
        private boolean _keep_runnging_after_exit;
        private int _scale;
        private boolean _withImageFetching;
        private boolean _websocket;
        private int _ws_elements;

        //private String _reg_password;
        //private String _reg_username;
        private String _uniqueComponentKey;
        private Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private DUUIPipelineComponent _component;

        // 这里？？
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            DUUISlurmDriver.ComponentInstance inst = _instances.poll();
            while (inst == null) {
                inst = _instances.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        //--------------------------------------------------------------------------------
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

            // _reg_password = comp.getDockerAuthPassword();
            // _reg_username = comp.getDockerAuthUsername();

            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }

        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        public String getUniqueComponentKey() {
            return _uniqueComponentKey;
        }

//        public String getPassword() {
//            return _reg_password;
//        }

        // public String getUsername() {
        //     return _reg_username;
        // }

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


    //O
    public static class Component {
        private DUUIPipelineComponent _component;

        public DUUISlurmDriver.Component withParameter(String key, String value) {
            _component.withParameter(key, value);
            return this;
        }

        public DUUISlurmDriver.Component withView(String viewName) {
            _component.withView(viewName);
            return this;
        }

        public DUUISlurmDriver.Component withSourceView(String viewName) {
            _component.withSourceView(viewName);
            return this;
        }

        public DUUISlurmDriver.Component withTargetView(String viewName) {
            _component.withTargetView(viewName);
            return this;
        }

        public Component(String target) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
            _component.withDriver(DUUISlurmDriver.class);
        }

        public DUUISlurmDriver.Component withDescription(String description) {
            _component.withDescription(description);
            return this;
        }

        public DUUISlurmDriver.Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

//        public DUUISlurmDriver.Component withRegistryAuth(String username, String password) {
//            _component.withDockerAuth(username, password);
//            return this;
//        }

//        public DUUISlurmDriver.Component withImageFetching() {
//            return withImageFetching(true);
//        }
//
//        public DUUISlurmDriver.Component withImageFetching(boolean imageFetching) {
//            _component.withDockerImageFetching(imageFetching);
//            return this;
//        }

//        public DUUISlurmDriver.Component withGPU(boolean gpu) {
//            _component.withDockerGPU(gpu);
//            return this;
//        }

//        public DUUISlurmDriver.Component withRunningAfterDestroy(boolean run) {
//            _component.withDockerRunAfterExit(run);
//            return this;
//        }

//        public DUUISlurmDriver.Component withWebsocket(boolean b) {
//            _component.withWebsocket(b);
//            return this;
//        }
//
//        public DUUISlurmDriver.Component withWebsocket(boolean b, int elements) {
//            _component.withWebsocket(b, elements);
//            return this;
//        }

        public DUUISlurmDriver.Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            _component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> DUUISlurmDriver.Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            _component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public DUUIPipelineComponent build() {
            return _component;
        }

//        public DUUISlurmDriver.Component withName(String name) {
//            _component.withName(name);
//            return this;
//        }


        //1
        public Component withSlurmJobName(String jobName) {
            _component.withSlurmJobName(jobName);
            return this;
        }

        //2
        public Component withSlurmImagePort(String port) {
            _component.withSlurmImagePort(port);
            return this;
        }


        //3
//        public Component withSlurmHostPort(String port) {
//            _component.withSlurmHostPort(port);
//            return this;
//        }

        //4
        public Component withSlurmRuntime(String time) {
            _component.withSlurmRuntime(time);
            return this;
        }

        //5
        public Component withSlurmCpus(String num) {
            _component.withSlurmCpus(num);
            return this;
        }

        //6
        public Component withSlurmMemory(String numG) {
            _component.withSlurmMemory(numG);
            return this;
        }

        //7
        public Component withSlurmOutPutLocation(String loc) {
            _component.withSlurmOutPutLocation(loc);
            return this;
        }

        //8
        public Component withSlurmErrorLocation(String loc) {
            _component.withSlurmErrorLocation(loc);
            return this;
        }

        //9
        public Component withSlurmSaveIn(String saveTo) {
            _component.withSlurmSaveIn(saveTo);
            return this;
        }

        //10
        public Component withSlurmGPU(String num) {
            _component.withSlurmGPU(num);
            return this;
        }

        //11
        public Component withSlurmSIFName(String sifName) {
            _component.withSlurmSIFName(sifName);
            return this;

        }

        //12
        public Component withSlurmEntryLocation(String loc) {
            _component.withSlurmEntryLocation(loc);
            return this;

        }


    }

//--------------------------------------------------------------------------


    @Override
    public void shutdown() {

    }
}


