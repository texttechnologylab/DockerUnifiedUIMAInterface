import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.exception.DockerClientException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.core.command.PushImageResultCallback;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

class PullImageStdout extends PullImageResultCallback {
    private String _status;
    PullImageStdout() {
        _status = "";
    }

    @Override
    public void onNext(PullResponseItem item) {
        if(item.getStatus()!=null) {
            _status = item.getStatus();
        }

        if(item.getProgressDetail()!=null) {
            if(item.getProgressDetail().getCurrent()!=null && item.getProgressDetail().getTotal()!=null) {
                System.out.printf("%s: %.2f%%\n", _status, ((float)item.getProgressDetail().getCurrent()/(float)item.getProgressDetail().getTotal())*100);
            }
            else {
                System.out.printf("%s.\n", _status);
            }
        }
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
        if(item.getStream()!=null) {
            System.out.print(item.getStream());
        }
    }

    /**
     * Awaits the image id from the response stream.
     *
     * @throws DockerClientException
     *             if the build fails.
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
     * @throws DockerClientException
     *             if the build fails or the timeout occurs.
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
     * @throws IOException
     */
    public DUUIDockerInterface() throws IOException {
        _docker = DockerClientBuilder.getInstance().build();
    }

    /**
     * Extracts port mapping from the container with the given containerid, this is important since docker does auto allocate
     * ports when not explicitly specifying the port number. This will only work on a DockerWrapper constructed container.
     * @param containerid The running containerid to read the port mapping from
     * @return The port it was mapped to.
     * @throws InterruptedException
     */
    public int extract_port_mapping(String containerid) throws InterruptedException {
        InspectContainerResponse container
                = _docker.inspectContainerCmd(containerid).exec();

        int innerport = 0;
        for(Map.Entry<ExposedPort, Ports.Binding[]> port : container.getNetworkSettings().getPorts().getBindings().entrySet()) {
            if(port.getValue().length > 0 && port.getKey().getPort() == 9714) {
                innerport = Integer.parseInt(port.getValue()[0].getHostPortSpec());
            }
        }

        System.out.printf("Detected port: %d\n",innerport);
        return innerport;
    }

    /**
     * Returns true if the code is run inside the container and false otherwise.
     * @return true if in container false otherwise
     */
    public boolean inside_container() {
        if(new File("/.dockerenv").exists()) {
            return true;
        }
        return false;
    }

    /**
     * Reads the container gateway bridge ip if inside the container to enable communication between sibling containers or the
     * localhost ip if one is the host.
     * @return The ip address.
     */
    public String get_ip() {
        if(inside_container()) {
            Network net = _docker.inspectNetworkCmd().withNetworkId("docker_gwbridge").exec();
            return net.getIpam().getConfig().get(0).getGateway();
        }
        return "127.0.0.1";
    }

    /**
     * Reads the logs from the container to determine if the container has started up without errors
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
        for(String x : logs) {
            completelog+=x;
        }
        return completelog;
    }

    /**
     * Stops the container with the given container id
     * @param id The id of the container to stop.
     */
    public void stop_container(String id) {
        _docker.stopContainerCmd(id).withTimeout(10).exec();
    }

    /**
     * Stops the container with the given container id
     * @param id The id of the container to stop.
     */
    public void rm_service(String id) {
        System.out.printf("Stopping service %s\n",id);
        _docker.removeServiceCmd(id).withServiceId(id).exec();
    }
    /**
     * Exports a running container to a new image.
     * @param containerid The containerid to commit to a new image
     * @param imagename The image name in the format "repository!imagename"
     */
    public void export_to_new_image(String containerid, String imagename) {
        if(!imagename.equals("")) {
            String split[] = imagename.split("!");
            _docker.commitCmd(containerid).withRepository(split[0]).withTag(split[1]).exec();
        }
    }

    public String run_service(String imagename, int scale, String tag) throws InterruptedException {
        if(tag==null) {
            tag = "localhost/reproducibleanno";
        }
        _docker.tagImageCmd(imagename,tag,imagename).exec();
        _docker.pushImageCmd(tag)
                .withTag(imagename)
                .exec(new PushImageResultCallback())
                .awaitCompletion(90, TimeUnit.SECONDS);
        ServiceSpec spec = new ServiceSpec();
        ServiceModeConfig cfg = new ServiceModeConfig();
        ServiceReplicatedModeOptions opts = new ServiceReplicatedModeOptions();
        cfg.withReplicated(opts.withReplicas(scale));
        spec.withMode(cfg);

        TaskSpec task = new TaskSpec();
        ContainerSpec cont = new ContainerSpec();
        cont = cont.withImage(tag+":"+imagename);
        task.withContainerSpec(cont);

        spec.withTaskTemplate(task);
        EndpointSpec end = new EndpointSpec();
        List<PortConfig> portcfg = new LinkedList<>();
        portcfg.add(new PortConfig().withTargetPort(9714).withPublishMode(PortConfig.PublishMode.ingress));
        end.withPorts(portcfg);
        spec.withEndpointSpec(end);

        System.out.printf("Spawning %d service replicas",scale);
        CreateServiceResponse cmd = _docker.createServiceCmd(spec).exec();
        return cmd.getId();
    }

    public int extract_service_port_mapping(String service) throws InterruptedException {
        Thread.sleep(1000);
        Service cmd = _docker.inspectServiceCmd(service).exec();
        Endpoint end = cmd.getEndpoint();
        for(PortConfig p : end.getPorts()) {
            return p.getPublishedPort();
        }
        return -1;
    }

    public String build(Path builddir, List<String> buildArgs) {
        BuildImageCmd buildCmd = _docker.buildImageCmd().withPull(true)
                .withBaseDirectory(builddir.toFile())
                .withDockerfile(Paths.get(builddir.toString(),"dockerfile").toFile());

        for (String buildArg : buildArgs) {
            String[] fields = buildArg.split("=", 2);
            String key = fields[0].trim();
            String value = fields[1].trim();
            buildCmd.withBuildArg(key, value);
        }

        String img_id = buildCmd.exec(new BuildImageProgress()).awaitImageId();
        return img_id;
    }

    public String pullImage(String tag) throws InterruptedException {
        _docker.pullImageCmd(tag).exec(new PullImageStdout()).awaitCompletion();
        System.out.printf("Pulled image with id %s\n",tag);
        return tag;
    }

    /**
     * Builds and runs the container with a specified temporary build directory and some flags.
     * @param gpu If the gpu should be used
     * @param autoremove If the autoremove flag is set for the container
     * @return The docker container id
     * @throws InterruptedException
     */
    public String run(String imageid, boolean gpu, boolean autoremove) throws InterruptedException {
        HostConfig cfg = new HostConfig();
        if(autoremove) {
            cfg = cfg.withAutoRemove(true);
        }
        if(gpu) {
            cfg = cfg.withDeviceRequests(ImmutableList.of(new DeviceRequest()
                    .withCapabilities(ImmutableList.of(ImmutableList.of("gpu")))));
        }

        CreateContainerCmd cmd = _docker.createContainerCmd(imageid)
                .withHostConfig(cfg)
                .withExposedPorts(ExposedPort.tcp(9714)).withPublishAllPorts(true);

        CreateContainerResponse feedback = cmd.exec();
        _docker.startContainerCmd(feedback.getId()).exec();
        return feedback.getId();
    }
}
