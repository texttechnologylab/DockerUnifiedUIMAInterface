package org.texttechnologylab.DockerUnifiedUIMAInterface;

import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.util.CompressArchiveUtil;
import io.prometheus.client.exporter.HTTPServer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.String.format;

class DUUIPrometheusThread extends Thread {
    private int _port;

    DUUIPrometheusThread(int openPort) {
        _port = openPort;
    }

    @Override
    public void run() {
        try {
            HTTPServer server = new HTTPServer(_port,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

public class DUUIPrometheusInterface {
    private int _prometheusExposedDataPort;
    private int _prometheusPort;
    private String _configurationYML;
    private Thread _prometheusThread;
    private DUUIDockerInterface _docker;

    DUUIPrometheusInterface(int prometheusExposedDataPort) {
        _prometheusThread = new DUUIPrometheusThread(prometheusExposedDataPort);
        _prometheusThread.start();
        _prometheusExposedDataPort = prometheusExposedDataPort;
        _prometheusPort = -1;
    }

    public void shutdown() {
        _prometheusThread.interrupt();
    }

    DUUIPrometheusInterface withAutoStartPrometheus(int prometheusPort) throws IOException, InterruptedException, URISyntaxException {
        return withAutoStartPrometheus(prometheusPort,null);
    }

    String generateURL() throws UnknownHostException {
        if(_prometheusPort==-1) {
            return null;
        }
        InetAddress IP = InetAddress.getLocalHost();
        return format("http://%s:%d",IP.getHostAddress().toString(),_prometheusPort);
    }

    String generateExposedAddrURL() throws UnknownHostException {
        InetAddress IP = InetAddress.getLocalHost();
        return format("host.docker.internal:%d",_prometheusExposedDataPort);
    }

    void copyConfigToContainer(String containerId) throws IOException, URISyntaxException {
        Path configuration = Files.createTempDirectory("duui");
        configuration = Paths.get(configuration.toString(),"prometheus.yml");
        if(_configurationYML == null) {
            _configurationYML = format(Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/prometheus.yml").toURI())),generateExposedAddrURL());
        }
        Files.write(configuration,_configurationYML.getBytes(StandardCharsets.UTF_8));
        System.out.println(format("[PrometheusInterface] Wrote temporary prometheus configuration file into %s.",configuration.toString()));

        Path temp = Files.createTempFile("", ".tar.gz");
        CompressArchiveUtil.tar(configuration, temp, true, false);
        InputStream uploadStream = Files.newInputStream(temp);
        _docker.getDockerClient()
                .copyArchiveToContainerCmd(containerId)
                .withTarInputStream(uploadStream)
                .withRemotePath("/etc/prometheus/")
                .exec();
    }

    DUUIPrometheusInterface withAutoStartPrometheus(int prometheusPort, String configurationYML) throws IOException, InterruptedException, URISyntaxException {
        if(_docker == null) {
            _docker = new DUUIDockerInterface();
        }
        _prometheusPort = prometheusPort;
        _configurationYML = configurationYML;

        try {
            InspectContainerResponse result = _docker.getDockerClient().inspectContainerCmd("duui_prometheus_auto_deploy").exec();
            int mapping = _docker.extract_port_mapping(result.getId(),9090);
            if(result.getState().getRunning()) {
                System.out.println("[PrometheusInterface] Found existing prometheus container which is running already.");
                System.out.println("[PrometheusInterface] WARNING: Since the container is already running, the implementation is unable to adjust the prometheus targets. If target adjusting is necessary please stop the container and restart the programm.");
                System.out.printf("[PrometheusInterface] Adjusting exposed port %d to running container port %d\n",_prometheusPort,mapping);
                _prometheusPort = mapping;
                return this;
            }

            copyConfigToContainer(result.getId());
            System.out.println("[PrometheusInterface] Found existing prometheus container starting it now...");
            _docker.getDockerClient().startContainerCmd(result.getId()).exec();
            _prometheusPort = _docker.extract_port_mapping(result.getId(),9090);
            System.out.printf("[PrometheusInterface] Prometheus panel is opened at %s\n",generateURL());
        }
        catch(Exception e) {
            _docker.pullImage("prom/prometheus:latest",null,null);
            System.out.println("[PrometheusInterface] Could not find existing container creating one...");
            ExposedPort tcp9090 = ExposedPort.tcp(9090);

            Ports portBindings = new Ports();
            portBindings.bind(tcp9090, Ports.Binding.bindPort(prometheusPort));


            CreateContainerResponse container = _docker.getDockerClient().createContainerCmd("prom/prometheus")
                    .withExposedPorts(tcp9090)
                    .withHostConfig(new HostConfig()
                            .withPortBindings(portBindings)
                            .withExtraHosts("host.docker.internal:host-gateway"))
                    .withName("duui_prometheus_auto_deploy")
                    .exec();
            copyConfigToContainer(container.getId());
            _docker.getDockerClient().startContainerCmd(container.getId()).exec();
            System.out.printf("[PrometheusInterface] Prometheus panel is opened at %s\n",generateURL());
        }
        return this;
    }
}
