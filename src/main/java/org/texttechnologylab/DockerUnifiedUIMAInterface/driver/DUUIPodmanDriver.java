package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.ImageException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;
import podman.client.PodmanClient;
import podman.client.containers.ContainerCreateOptions;
import podman.client.containers.ContainerDeleteOptions;
import podman.client.containers.ContainerInspectOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static org.awaitility.Awaitility.await;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.getLocalhost;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.responsiveAfterTime;

/**
 * Driver for using a local Podman instance to run DUUI components
 *
 * @author Giuseppe Abrami
 */
public class DUUIPodmanDriver implements IDUUIDriverInterface {

    private PodmanClient _interface = null;
    private HttpClient _client;

    private Vertx _vertx = null;
    private DUUILuaContext _luaContext = null;
    private int _container_timeout;

    private HashMap<String, DUUIDockerDriver.InstantiatedComponent> _active_components;


    public DUUIPodmanDriver() throws IOException, SAXException {

        VertxOptions vertxOptions = new VertxOptions().setPreferNativeTransport(true);
        _vertx = Vertx.vertx(vertxOptions);
        _client = HttpClient.newHttpClient();

        System.out.printf("[PodmanDriver] Is Native Transport Enabled: %s\n", _vertx.isNativeTransportEnabled());

        PodmanClient.Options options = new PodmanClient.Options().setSocketPath(podmanSocketPath());

        _interface = PodmanClient.create(_vertx, options);
        _container_timeout = 10;
        _active_components = new HashMap<String, DUUIDockerDriver.InstantiatedComponent>();
        _luaContext = null;

    }

    public static String podmanSocketPath() {
        String path = System.getenv("PODMAN_SOCKET_PATH");

        if (path == null) {
            String uid = System.getenv("UID");
            if (uid == null) {
                try {
                    ProcessBuilder pb = new ProcessBuilder("id", "-u");
                    Process process = pb.start();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    uid = reader.readLine(); // UID aus der Ausgabe lesen
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            path = "/run/user/" + uid + "/podman/podman.sock";
            System.out.println(path);
        }

        return path;
    }

    private static <T> T awaitResult(Future<T> future) throws Throwable {
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<T> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        future.onComplete(res -> {
            if (res.succeeded()) {
                result.set(res.result());
            } else {
                failure.set(res.cause());
            }
            done.set(true);
        });
        await().untilTrue(done);
        if (failure.get() != null) {
            throw failure.get();
        } else {
            return result.get();
        }
    }

    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        this._luaContext = luaContext;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        return component.getDockerImageName() != null;
    }

    public static void pull(String sImagename) throws ImageException {

//        _interface.images().pull(sImagename, new ImagePullOptions())
//                .subscribe(new Flow.Subscriber<JsonObject>() {
//            @Override
//            public void onSubscribe(Flow.Subscription subscription) {
//                System.out.println(subscription.toString());
//            }
//
//            @Override
//            public void onNext(JsonObject item) {
//                System.out.println(item.toString());
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                throwable.printStackTrace();
//            }
//
//            @Override
//            public void onComplete() {
//                System.out.println("finish");
//            }
//        });


        ProcessBuilder pb = new ProcessBuilder("podman", "pull", sImagename);
        Process process = null;

        try {
            process = pb.start();
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                BufferedReader brError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String input;
                while ((input = br.readLine()) != null) {
                    // Print the input
                    System.out.println(input);
                }
                StringBuilder sb = new StringBuilder();
                while ((input = brError.readLine()) != null) {
                    // Print the input
                    if (sb.length() > 0) {
                        sb.append("\n");
                    }
                    sb.append(input);
                }
                if (sb.length() > 0) {
                    throw new ImageException(sb.toString());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            process.waitFor();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public void printConcurrencyGraph(String uuid) {

    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {

        String uuid = UUID.randomUUID().toString();
        while (_active_components.containsKey(uuid.toString())) {
            uuid = UUID.randomUUID().toString();
        }


        DUUIDockerDriver.InstantiatedComponent comp = new DUUIDockerDriver.InstantiatedComponent(component);

        // Inverted if check because images will never be pulled if !comp.getImageFetching() is checked.
        if (comp.getImageFetching()) {
            if (comp.getUsername() != null) {
                System.out.printf("[PodmanDriver] Attempting image %s download from secure remote registry\n", comp.getImageName());
            }
            try {
                pull(comp.getImageName());
//            _interface.images().pull(comp.getImageName(), new ImagePullOptions());

                if (shutdown.get()) {
                    return null;
                }

                System.out.printf("[PodmanDriver] Pulled image with id %s\n", comp.getImageName());
            } catch (ImageException e) {
                System.err.println(e.getMessage());
            }


        } else {
//            _interface.pullImage(comp.getImageName());
            try {
                if (!awaitResult(_interface.images().exists(comp.getImageName()))) {
                    throw new InvalidParameterException(format("Could not find local image \"%s\". Did you misspell it or forget with .withImageFetching() to fetch it from remote registry?", comp.getImageName()));
                }
            } catch (Exception e) {
                throw e;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        try {
            if (awaitResult(_interface.images().exists(comp.getImageName()))) {
                System.out.printf("[PodmanDriver] Assigned new pipeline component unique id %s\n", uuid);

                _active_components.put(uuid, comp);

                for (int i = 0; i < comp.getScale(); i++) {
                    if (shutdown.get()) {
                        return null;
                    }


                    ContainerCreateOptions pOptions = new ContainerCreateOptions();
                    pOptions.image(comp.getImageName());
                    pOptions.remove(true);
                    pOptions.publishImagePorts(true);

                    if (comp.usesGPU()) {
                        List<ContainerCreateOptions.LinuxDevice> linuxDevices = new ArrayList<>();
                        linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 0, "/dev/nvidia0", "c", 0));
                        //                linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 195, 255, "/dev/nvidiactl", "c", 0));
                        //                linuxDevices.add(new ContainerCreateOptions.LinuxDevice(0666, 0, 236, 0, "/dev/nvidia-uvm", "c", 0));

                        //                pOptions.devices(linuxDevices);
                        pOptions.hostDeviceList(linuxDevices);
                    }


                    JsonObject pObject = null;
                    JsonObject iObject = null;
                    String containerId = "";
                    int port = -1;
                    try {
                        pObject = awaitResult(_interface.containers().create(pOptions));
                        containerId = pObject.getString("Id");

                        _interface.containers().start(containerId);

                        System.out.println(pObject);


                        iObject = awaitResult(_interface.containers().inspect(containerId, new ContainerInspectOptions().setSize(false)));
                        JSONObject nObject = new JSONObject(iObject);
                        System.out.println(nObject);
                        port = nObject.getJSONObject("map").getJSONObject("HostConfig").getJSONObject("PortBindings").getJSONArray("9714/tcp").getJSONObject(0).getInt("HostPort");


                    } catch (Throwable e) {
                        e.printStackTrace();
                        stop_container(containerId, true);
                        throw new RuntimeException(e);
                    }

                    try {
                        if (port == 0) {
                            throw new UnknownError("Could not read the container port!");
                        }
                        final int iCopy = i;
                        final String uuidCopy = uuid;
                        IDUUICommunicationLayer layer = responsiveAfterTime(getLocalhost() + ":" + String.valueOf(port), jc, _container_timeout, _client, (msg) -> {
                            System.out.printf("[PodmanDriver][%s][Podman Replication %d/%d] %s\n", uuidCopy, iCopy + 1, comp.getScale(), msg);
                        }, _luaContext, skipVerification);
                        System.out.printf("[PodmanDriver][%s][Podman Replication %d/%d] Container for image %s is online (URL http://127.0.0.1:%d) and seems to understand DUUI V1 format!\n", uuid, i + 1, comp.getScale(), comp.getImageName(), port);

                        /// Add one replica of the instantiated component per worker
                        for (int j = 0; j < comp.getWorkers(); j++) {
                            comp.addInstance(new DUUIDockerDriver.ComponentInstance(containerId, port, layer.copy()));
                        }
                    } catch (Exception e) {

                        e.printStackTrace();
                        //throw e;
                    }


                }

            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return shutdown.get() ? null : uuid;
    }

    private void stop_container(String containerId) {
        stop_container(containerId, true);
    }

    private void stop_container(String containerId, boolean bDelete) {
        _interface.containers().stop(containerId, false, 1);
        if (bDelete) {
            _interface.containers().delete(containerId, new ContainerDeleteOptions().setTimeout(1).setIgnore(true));
        }
    }

    @Override
    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        DUUIDockerDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
    }

    /**
     * init reader component
     * @param uuid
     * @param filePath
     * @return
     */
    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        DUUIDockerDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        return IDUUIInstantiatedPipelineReaderComponent.initComponent(comp, filePath);
    }

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer) throws CASException, PipelineComponentException, CompressorException, CommunicationLayerException, IOException {
        long mutexStart = System.nanoTime();
        DUUIDockerDriver.InstantiatedComponent comp = _active_components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        IDUUIInstantiatedPipelineComponent.process(aCas, comp, perf);

    }

    @Override
    public boolean destroy(String uuid) {
        DUUIDockerDriver.InstantiatedComponent comp = _active_components.remove(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        if (!comp.getRunningAfterExit()) {
            int counter = 1;
            for (DUUIDockerDriver.ComponentInstance inst : comp.getInstances()) {
                System.out.printf("[PodmanDriver][Replication %d/%d] Stopping docker container %s...\n", counter, comp.getInstances().size(), inst.getContainerId());
                stop_container(inst.getContainerId(), true);

                counter += 1;
            }
        }

        return true;
    }

    @Override
    public void shutdown() {
        for (String s : _active_components.keySet()) {
            destroy(s);
        }
        try {
            Thread.sleep(3000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Component {
        private DUUIPipelineComponent _component;

        public Component(String target) throws URISyntaxException, IOException {
            _component = new DUUIPipelineComponent();
            _component.withDockerImageName(target);
        }

        public Component(DUUIPipelineComponent pComponent) throws URISyntaxException, IOException {
            _component = pComponent;
        }

        public DUUIPodmanDriver.Component withParameter(String key, String value) {
            _component.withParameter(key, value);
            return this;
        }

        public DUUIPodmanDriver.Component withView(String viewName) {
            _component.withView(viewName);
            return this;
        }

        public DUUIPodmanDriver.Component withSourceView(String viewName) {
            _component.withSourceView(viewName);
            return this;
        }

        public DUUIPodmanDriver.Component withTargetView(String viewName) {
            _component.withTargetView(viewName);
            return this;
        }

        public DUUIPodmanDriver.Component withDescription(String description) {
            _component.withDescription(description);
            return this;
        }

        /**
         * Start the given number of parallel instances (containers).
         * @param scale Number of containers to start.
         * @return {@code this}
         */
        public DUUIPodmanDriver.Component withScale(int scale) {
            _component.withScale(scale);
            return this;
        }

        /**
         * Set the maximum concurrency-level of each component by instantiating the multiple replicas per container.
         * @param workers Number of replicas per container.
         * @return {@code this}
         */
        public DUUIPodmanDriver.Component withWorkers(int workers) {
            _component.withWorkers(workers);
            return this;
        }

        public DUUIPodmanDriver.Component withRegistryAuth(String username, String password) {
            _component.withDockerAuth(username, password);
            return this;
        }

        public DUUIPodmanDriver.Component withImageFetching() {
            return withImageFetching(true);
        }

        public DUUIPodmanDriver.Component withImageFetching(boolean imageFetching) {
            _component.withDockerImageFetching(imageFetching);
            return this;
        }

        public DUUIPodmanDriver.Component withGPU(boolean gpu) {
            _component.withDockerGPU(gpu);
            return this;
        }

        public DUUIPodmanDriver.Component withRunningAfterDestroy(boolean run) {
            _component.withDockerRunAfterExit(run);
            return this;
        }

        public DUUIPodmanDriver.Component withSegmentationStrategy(DUUISegmentationStrategy strategy) {
            _component.withSegmentationStrategy(strategy);
            return this;
        }

        public <T extends DUUISegmentationStrategy> DUUIPodmanDriver.Component withSegmentationStrategy(Class<T> strategyClass) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
            _component.withSegmentationStrategy(strategyClass.getDeclaredConstructor().newInstance());
            return this;
        }

        public DUUIPipelineComponent build() {
            _component.withDriver(DUUIPodmanDriver.class);
            return _component;
        }

        public DUUIPodmanDriver.Component withName(String name) {
            _component.withName(name);
            return this;
        }
    }

}
