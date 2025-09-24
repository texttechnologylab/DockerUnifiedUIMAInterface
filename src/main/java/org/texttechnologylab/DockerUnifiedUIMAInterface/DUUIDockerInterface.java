package org.texttechnologylab.DockerUnifiedUIMAInterface;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.exception.DockerClientException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

//TODO: Enable different tags here
class PullImageStdout extends PullImageResultCallback {
    private String _status;
    private final AtomicBoolean shutdown;

    PullImageStdout(AtomicBoolean shutdown) {
        _status = "";
        this.shutdown = shutdown;
    }

    @Override
    public void onNext(PullResponseItem item) {
        if (shutdown.get()) {
            onComplete();
            onError(new CancellationException("Pull has been cancelled."));
            return;
        }

        if (item.getStatus() != null) {
            _status = item.getStatus();
        }

        if (item.getProgressDetail() != null) {
            if (item.getProgressDetail().getCurrent() != null && item.getProgressDetail().getTotal() != null) {
                float progress = ((float) item.getProgressDetail().getCurrent() / (float) item.getProgressDetail().getTotal()) * 100;
            } else {
                System.out.printf("[DockerSwarmDriver] %s.\n", _status);
            }
        }
    }
}

class PushImageStdout extends ResultCallback.Adapter<PushResponseItem> {
    private String _status;

    PushImageStdout() {
        _status = "";
    }

    @Override
    public void onNext(PushResponseItem item) {
        if (item.getStatus() != null) {
            _status = item.getStatus();
        }

        if (item.getProgressDetail() != null) {
            if (item.getProgressDetail().getCurrent() != null && item.getProgressDetail().getTotal() != null) {
                System.out.printf("[DockerSwarmDriver] %s: %.2f%%\n", _status, ((float) item.getProgressDetail().getCurrent() / (float) item.getProgressDetail().getTotal()) * 100);
            } else {
                System.out.printf("[DockerSwarmDriver] %s.\n", _status);
            }
        }
        super.onNext(item);
    }
}

/**
 * This code is based on the code from the docker api package
 */
class BuildImageProgress extends BuildImageResultCallback {
    private String imageId;
    private String error;

    @Override
    public void onNext(BuildResponseItem item) {
        if (item.isBuildSuccessIndicated()) {
            this.imageId = item.getImageId();
        } else if (item.isErrorIndicated()) {
            this.error = item.getError();
        }
        if (item.getStream() != null) {
            System.out.print(item.getStream());
        }
    }

    /**
     * Awaits the image id from the response stream.
     *
     * @throws DockerClientException if the build fails.
     */
    public String awaitImageId() {
        try {
            awaitCompletion();
        } catch (InterruptedException e) {
            throw new DockerClientException("", e);
        }

        return getImageId();
    }

    /**
     * Awaits the image id from the response stream.
     *
     * @throws DockerClientException if the build fails or the timeout occurs.
     */
    public String awaitImageId(long timeout, TimeUnit timeUnit) {
        try {
            awaitCompletion(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new DockerClientException("Awaiting image id interrupted: ", e);
        }

        return getImageId();
    }

    private String getImageId() {
        if (imageId != null) {
            return imageId;
        }

        if (error == null) {
            throw new DockerClientException("Could not build image");
        }

        throw new DockerClientException("Could not build image: " + error);
    }
}

/**
 * This is the general docker interface which interacts with the docker daemon.
 */
public class DUUIDockerInterface {
    /**
     * The connection to the docker client.
     */
    DockerClient _docker;

    /**
     * Creates a default object which connects to the local docker daemon, may need admin rights depending on the docker installation
     * Depending on the Operating System a different connection URI is used to build the DockerClient. On Windows the npipe protocol
     * is required to establish a connection with the docker daemon.
     *
     * @throws IOException
     */
    public DUUIDockerInterface() throws IOException {

        if (!System.getProperty("os.name").contains("Windows")) {
//            _docker = DockerClientBuilder.getInstance().build();
            var config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
            DockerHttpClient pClient = new ApacheDockerHttpClient.Builder()
                    .dockerHost(config.getDockerHost())
                    .sslConfig(config.getSSLConfig())
                    .maxConnections(100)
                    .connectionTimeout(Duration.ofSeconds(30))
                    .responseTimeout(Duration.ofSeconds(45))
                    .build();
            _docker = DockerClientImpl.getInstance(config, pClient);

        } else {
            // Windows
            DockerHttpClient http = null;
            try {
                http = new ApacheDockerHttpClient.Builder()
                        .connectionTimeout(Duration.ofSeconds(5))
                        .responseTimeout(Duration.ofMinutes(10))
                        .dockerHost(URI.create("npipe:////./pipe/docker_engine"))
                        .build();
            }
            catch (Exception e){
                http = new ApacheDockerHttpClient.Builder()
                        .connectionTimeout(Duration.ofSeconds(5))
                        .responseTimeout(Duration.ofMinutes(10))
                        .dockerHost(URI.create("tcp://127.0.0.1:2375")) // if npipe doesn't work.
                        .build();
            }
            _docker = DockerClientBuilder.getInstance()
                    .withDockerHttpClient(http)
                    .build();

        }
    }

    /**
     * Extracts port mapping from the container with the given containerid, this is important since docker does auto allocate
     * ports when not explicitly specifying the port number. This will only work on a DockerWrapper constructed container.
     *
     * @param containerid The running containerid to read the port mapping from
     * @return The port it was mapped to.
     * @throws InterruptedException
     */
    public int extract_port_mapping(String containerid) throws InterruptedException {
        InspectContainerResponse container
            = _docker.inspectContainerCmd(containerid).exec();

        int innerport = 0;
        for (Map.Entry<ExposedPort, Ports.Binding[]> port : container.getNetworkSettings().getPorts().getBindings().entrySet()) {
            if (port.getValue().length > 0 && port.getKey().getPort() == 9714) {
                innerport = Integer.parseInt(port.getValue()[0].getHostPortSpec());
            }
        }

        return innerport;
    }

    /**
     * Extracts port mapping from the container with the given containerid, this is important since docker does auto allocate
     * ports when not explicitly specifying the port number. This will only work on a DockerWrapper constructed container.
     *
     * @param containerid The running containerid to read the port mapping from
     * @return The port it was mapped to.
     * @throws InterruptedException
     */
    public int extract_port_mapping(String containerid, int portMapping) throws InterruptedException {
        InspectContainerResponse container
            = _docker.inspectContainerCmd(containerid).exec();

        int innerport = 0;
        for (Map.Entry<ExposedPort, Ports.Binding[]> port : container.getNetworkSettings().getPorts().getBindings().entrySet()) {
            if (port.getValue().length > 0 && port.getKey().getPort() == portMapping) {
                innerport = Integer.parseInt(port.getValue()[0].getHostPortSpec());
            }
        }

        return innerport;
    }

    public int extract_service_port_mapping(String service) throws InterruptedException {
        Thread.sleep(1000);
        Service cmd = _docker.inspectServiceCmd(service).exec();
        Endpoint end = cmd.getEndpoint();
        for (PortConfig p : end.getPorts()) {
            return p.getPublishedPort();
        }
        return -1;
    }

    /**
     * Returns true if the code is run inside the container and false otherwise.
     *
     * @return true if in container false otherwise
     */
    public boolean inside_container() {
        if (new File("/.dockerenv").exists()) {
            return true;
        }
        return false;
    }

    /**
     * Reads the container gateway bridge ip if inside the container to enable communication between sibling containers or the
     * localhost ip if one is the host.
     *
     * @return The ip address.
     */
    public String get_ip() {
        if (inside_container()) {
            Network net = _docker.inspectNetworkCmd().withNetworkId("docker_gwbridge").exec();
            return net.getIpam().getConfig().get(0).getGateway();
        }
        return "127.0.0.1";
    }

    /**
     * Reads the logs from the container to determine if the container has started up without errors
     *
     * @param containerid The container id to check the logs from
     * @return The string representation of the read logs
     * @throws InterruptedException
     */
    public String get_logs(String containerid) throws InterruptedException {
        final List<String> logs = new ArrayList<>();
        InspectContainerResponse container
            = _docker.inspectContainerCmd(containerid).exec();
        _docker.logContainerCmd(containerid).withContainerId(containerid).withStdOut(true).withStdErr(true)
            .withTimestamps(true).withTail(5).exec(new LogContainerResultCallback() {
                @Override
                public void onNext(Frame item) {
                    logs.add(item.toString());
                }
            }).awaitCompletion();
        String completelog = "";
        for (String x : logs) {
            completelog += x;
        }
        return completelog;
    }

    /**
     * Stops the container with the given container id
     *
     * @param id The id of the container to stop.
     */
    public void stop_container(String id) {
        try {
            _docker.stopContainerCmd(id).withTimeout(10).exec();
        } catch (NotModifiedException e) {
        } catch (Exception e) {
        }
    }

    /**
     * Stops the container with the given container id
     *
     * @param id The id of the container to stop.
     */
    public void rm_service(String id) {
        _docker.removeServiceCmd(id).withServiceId(id).exec();
    }

    /**
     * Exports a running container to a new image.
     *
     * @param containerid The containerid to commit to a new image
     * @param imagename   The image name in the format "repository!imagename"
     */
    public void export_to_new_image(String containerid, String imagename) {
        if (!imagename.equals("")) {
            String split[] = imagename.split("!");
            _docker.commitCmd(containerid).withRepository(split[0]).withTag(split[1]).exec();
        }
    }

    public Image getLocalImage(String imageName) {
        List<Image> images = _docker.listImagesCmd()
            .withShowAll(true)
            .exec();
        for (Image i : images) {
            if (i.getId() == imageName) {
                return i;
            }
            String[] repoTags = i.getRepoTags();
            if (repoTags != null) {
                for (String repo : repoTags) {
                    if (repo.equals(imageName)) {
                        return i;
                    }
                }
            }
        }
        return null;
    }

    public void push_image(String remoteName, String localName, String username, String password) throws InterruptedException {
        Image img = getLocalImage(localName);
        if (img == null) {
            throw new InvalidParameterException(format("Could not find local image %s, not attempting to upload it to a registry!", localName));
        }

        _docker.tagImageCmd(localName, remoteName, "latest").exec();
        if (username != null && password != null) {
            AuthConfig cfg = new AuthConfig();
            cfg.withPassword(password);
            cfg.withUsername(username);
            _docker.pushImageCmd(remoteName)
                .withAuthConfig(cfg)
                .exec(new PushImageStdout())
                .awaitCompletion();
        } else {
            _docker.pushImageCmd(remoteName)
                .exec(new PushImageStdout())
                .awaitCompletion();
        }
    }

    public String run_service(String imagename, int scale) throws InterruptedException {
        return run_service(imagename, scale, new ArrayList<>(0));
    }

    public String run_service(String imagename, int scale, List<String> constraints) throws InterruptedException {
        ServiceSpec spec = new ServiceSpec();
        ServiceModeConfig cfg = new ServiceModeConfig();
        ServiceReplicatedModeOptions opts = new ServiceReplicatedModeOptions();
        cfg.withReplicated(opts.withReplicas(scale));
        spec.withMode(cfg);


        TaskSpec task = new TaskSpec();
        ContainerSpec cont = new ContainerSpec();
        cont = cont.withImage(imagename);
        task.withContainerSpec(cont);
        if (constraints.size() > 0) {
            task.withPlacement(new ServicePlacement().withConstraints(constraints));
        }

        spec.withTaskTemplate(task);
        EndpointSpec end = new EndpointSpec();
        List<PortConfig> portcfg = new LinkedList<>();
        portcfg.add(new PortConfig().withTargetPort(9714).withPublishMode(PortConfig.PublishMode.ingress));
        end.withPorts(portcfg);
        spec.withEndpointSpec(end);

        CreateServiceResponse cmd = _docker.createServiceCmd(spec).exec();
        return cmd.getId();
    }

    public boolean hasLocalImage(String imageName) {
        InspectImageResponse resp = _docker.inspectImageCmd(imageName).exec();
        if (resp != null) {
            return true;
        }

        List<Image> images = _docker.listImagesCmd()
            .withShowAll(true)
            .exec();
        for (Image i : images) {
            if (i.getId() == imageName) {
                return true;
            }

            if (i.getRepoTags() != null) {
                for (String repo : i.getRepoTags()) {
                    if (repo.equals(imageName)) {
                        return true;
                    }
                }
            } else if (i.getRepoDigests() != null) {
                for (String dig : i.getRepoDigests()) {
                    if (dig.equals(imageName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public DockerClient getDockerClient() {
        return _docker;
    }

    public String build(Path builddir, List<String> buildArgs) {
        BuildImageCmd buildCmd = _docker.buildImageCmd().withPull(true)
            .withBaseDirectory(builddir.toFile())
            .withDockerfile(Paths.get(builddir.toString(), "dockerfile").toFile());

        for (String buildArg : buildArgs) {
            String[] fields = buildArg.split("=", 2);
            String key = fields[0].trim();
            String value = fields[1].trim();
            buildCmd.withBuildArg(key, value);
        }

        String img_id = buildCmd.exec(new BuildImageProgress()).awaitImageId();
        return img_id;
    }

    public boolean isSwarmManagerNode() {
        Info sw = _docker.infoCmd().exec();
        return sw.getSwarm().getControlAvailable();
    }


    public String pullImage(String tag) throws InterruptedException {
        return pullImage(tag, null, null);
    }


    public String pullImage(String tag, String username, String password) throws InterruptedException {
        return pullImage(tag, username, password, null);
    }

    /**
     * Attempts to pull a docker image using its tag, and credentials if necessary.
     *
     * @param tag      The unique identifier of the image (uri:version).
     * @param username Credentials username.
     * @param password Credentials password.
     * @param shutdown A flag to signal a pull cancel.
     * @return The image tag.
     * @throws InterruptedException
     */
    public String pullImage(String tag, String username, String password, AtomicBoolean shutdown) throws InterruptedException {

        try {
            if (username != null && password != null) {
                AuthConfig cfg = new AuthConfig();
                cfg.withUsername(username);
                cfg.withPassword(password);
                ResultCallbackTemplate temp = _docker.pullImageCmd(tag)
                    .withAuthConfig(cfg)
                    .exec(new PullImageStdout(shutdown));
                temp.onError(new Exception());
                temp.awaitCompletion();
            } else {

                ResultCallbackTemplate template = _docker.pullImageCmd(tag)
                    .exec(new PullImageStdout(shutdown));

                // What is the purpose of throwing an Exception every time?
                template.awaitCompletion();
            }
        } catch (CancellationException exception) {
            shutdown.set(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Could not fetch image %s, continuing without.\n", tag);
        }
        return tag;
    }

    public String getDigestFromImage(String imagename) {

        if (imagename.split(":").length == 1) {
            imagename = imagename + ":latest";
        }
        InspectImageResponse resp = _docker.inspectImageCmd(imagename).exec();
        List<String> digests = resp.getRepoDigests();

        for (String i : digests) {
            return i;
        }
        return null;
    }


    /**
     * Run the given Docker image in a container.
     *
     * @param imageid    The Docker image name
     * @param gpu        If true, enable {@link DeviceRequest#withCapabilities(List) GPU capabilities}
     * @param autoremove If true, set the auto-remove flag for this container
     * @param port       Port to {@link CreateContainerCmd#withExposedPorts(ExposedPort...) expose} in the container
     *                   If set to {@code null}, the {@code portContainer} will still be exposed but not mapped.
     * @param mapDaemon  If true, {@link HostConfig#withBinds(Binds) bind} (volume mount) the Docker socket from the
     *                   host machine (/var/run/docker.sock)
     * @return The docker container id
     * @throws InterruptedException
     */
    public String run(String imageid, boolean gpu, boolean autoremove, int port, boolean mapDaemon) throws InterruptedException {
        return run(imageid, null, gpu, autoremove, port, null, mapDaemon);
    }

    /**
     * Run the given Docker image in a container.
     *
     * @param imageid    The Docker image name
     * @param env        A list of environment variables to set at runtime
     * @param gpu        If true, enable {@link DeviceRequest#withCapabilities(List) GPU capabilities}
     * @param autoremove If true, set the auto-remove flag for this container
     * @param port       Port to {@link CreateContainerCmd#withExposedPorts(ExposedPort...) expose} in the container
     *                   If set to {@code null}, the {@code portContainer} will still be exposed but not mapped.
     * @param mapDaemon  If true, {@link HostConfig#withBinds(Binds) bind} (volume mount) the Docker socket from the
     *                   host machine (/var/run/docker.sock)
     * @return The docker container id
     * @throws InterruptedException
     */
    public String run(String imageid, List<String> env, boolean gpu, boolean autoremove, int port, boolean mapDaemon) throws InterruptedException {
        return run(imageid, env, gpu, autoremove, port, null, mapDaemon);
    }

    /**
     * Run the given Docker image in a container.
     *
     * @param imageid       The Docker image name
     * @param gpu           If true, enable {@link DeviceRequest#withCapabilities(List) GPU capabilities}
     * @param autoremove    If true, set the auto-remove flag for this container
     * @param portContainer Port to {@link CreateContainerCmd#withExposedPorts(ExposedPort...) expose} in the container
     * @param portHost      {@link HostConfig#withPortBindings(Ports) Bind} the given port on the host to {@code portContainer}.
     *                      If set to {@code null}, the {@code portContainer} will still be exposed but not mapped.
     * @param mapDaemon     If true, {@link HostConfig#withBinds(Binds) bind} (volume mount) the Docker socket from the
     *                      host machine (/var/run/docker.sock)
     * @return The docker container id
     * @throws InterruptedException
     */
    public String run(String imageid, boolean gpu, boolean autoremove, int portContainer, int portHost, boolean mapDaemon) throws InterruptedException {
        return run(imageid, null, gpu, autoremove, portContainer, portHost, mapDaemon);
    }

    /**
     * Run the given Docker image in a container.
     *
     * @param imageid       The Docker image name
     * @param env           A list of environment variables to set at runtime
     * @param gpu           If true, enable {@link DeviceRequest#withCapabilities(List) GPU capabilities}
     * @param autoremove    If true, set the auto-remove flag for this container
     * @param portContainer Port to {@link CreateContainerCmd#withExposedPorts(ExposedPort...) expose} in the container
     * @param portHost      {@link HostConfig#withPortBindings(Ports) Bind} the given port on the host to {@code portContainer}.
     *                      If set to {@code null}, the {@code portContainer} will still be exposed but not mapped.
     * @param mapDaemon     If true, {@link HostConfig#withBinds(Binds) bind} (volume mount) the Docker socket from the
     *                      host machine (/var/run/docker.sock)
     * @return The docker container id
     * @throws InterruptedException
     */
    public String run(String imageid, List<String> env, boolean gpu, boolean autoremove, int portContainer, Integer portHost, boolean mapDaemon) throws InterruptedException {
        HostConfig cfg = new HostConfig()
                .withPublishAllPorts(true);

        if (autoremove) {
            cfg = cfg.withAutoRemove(true);
        }
        if (gpu) {
            cfg = cfg.withDeviceRequests(ImmutableList.of(new DeviceRequest()
                .withCapabilities(ImmutableList.of(ImmutableList.of("gpu")))));
        }

        if (!Objects.isNull(portHost) && portHost > 0) {
            cfg.withPortBindings(new PortBinding(new Ports.Binding(null, String.valueOf(portHost)), new ExposedPort(portContainer)));
        }

        if (mapDaemon) {
            cfg = cfg.withBinds(Bind.parse("/var/run/docker.sock:/var/run/docker.sock"));
        }

        CreateContainerCmd cmd = _docker.createContainerCmd(imageid)
            .withHostConfig(cfg)
            .withExposedPorts(ExposedPort.tcp(portContainer));

        if (!Objects.isNull(env) && !env.isEmpty()) {
            cmd = cmd.withEnv(env);
        }

        CreateContainerResponse feedback = cmd.exec();
        _docker.startContainerCmd(feedback.getId()).exec();
        return feedback.getId();
    }
}
