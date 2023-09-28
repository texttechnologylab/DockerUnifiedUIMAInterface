package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation;

import static java.lang.String.format;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceView;

import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.StatsCmd;
import com.github.dockerjava.api.command.WaitContainerResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.InvocationBuilder.AsyncResultCallback;

public class DUUISimpleMonitor implements IDUUIMonitor, IDUUIResource {
    
    final static HttpClient _client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.ALWAYS)
        .proxy(ProxySelector.getDefault())
        .executor(Runnable::run)
        .connectTimeout(Duration.ofSeconds(1000)).build();

    // final DUUIRestClient _handler = DUUIRestClient.getInstance(); 
    public final static String V1_MONITOR_PIPELINE_UPDATE = "/v1/pipeline_measurement";
    public final static String V1_MONITOR_DOCUMENT_UPDATE = "/v1/document_table";
    public final static String V1_MONITOR_DOCUMENT_MEASUREMENT_UPDATE = "/v1/document_measurement";
    public final static String V1_MONITOR_GRAPH_UPDATE = "/v1/graph_update";
    public final static String V1_MONITOR_SYSTEM_DYNAMIC_INFO = "/v1/system_dynamic_info";
    public final static String V1_MONITOR_SYSTEM_STATIC_INFO = "/v1/system_static_info";
    public final static String V1_MONITOR_RESOURCE_INFO = "/v1/system_resource_info";
    public final static String V1_MONITOR_STATUS = "/v1/status";

    final InetAddress _url;
    final DUUIDockerInterface _docker;
    String container_id = null;
    final String _image_id = "duui_monitor";
    Map<String, Object> containerStats = new ConcurrentHashMap<>();
    DockerContainerView view;
        
    int _port = 8086;

    JSONObject statsJson = new JSONObject();

    public DUUISimpleMonitor() throws IOException {

        _docker = new DUUIDockerInterface();
        _url = InetAddress.getLocalHost();
    }
    
    public DUUISimpleMonitor setup() throws InterruptedException, UnknownHostException {
        try {
            InspectContainerResponse result = _docker.getDockerClient().inspectContainerCmd(_image_id).exec();
            container_id = result.getId();
            int mapping = _docker.extract_port_mapping(result.getId(),8086);
            if (result.getState().getRunning()) {
                _port = mapping;
                System.out.println("[SimpleMonitor] Using running container...");
            } else {
                _docker.getDockerClient().startContainerCmd(result.getId()).exec();
                _port = _docker.extract_port_mapping(result.getId(), 8086);
            }
        } catch (Exception e) {
            _docker.pullImage(_image_id,null,null);
            System.out.println("[SimpleMonitor] Could not find existing container creating one...");
            ExposedPort tcp8086 = ExposedPort.tcp(8086);
            
            Ports portBindings = new Ports();
            portBindings.bind(tcp8086, Ports.Binding.bindPort(_port));
            
            
            CreateContainerResponse container = _docker.getDockerClient().createContainerCmd(_image_id)
            .withExposedPorts(tcp8086)
            .withHostConfig(new HostConfig()
            .withPortBindings(portBindings))
            .withName(_image_id)
            .exec();
            container_id = container.getId();
            // _docker.getDockerClient().startContainerCmd(container.getId()).exec();
        }

        System.out.printf("[SimpleMonitor] Monitor dashboard panel is opened at %s\n", generateURL());
        Thread.sleep(2000);
        view = new DockerContainerView(container_id, _image_id);

        return this;
    }

    public void sendUpdate(Map<String, Object> updateMap, String type) throws IOException, InterruptedException {
        JSONObject update = new JSONObject(updateMap);
        sendUpdate(update, type);
    }

    public void sendUpdate(JSONObject update, String type) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(generateURL() + type))
            .POST(HttpRequest.BodyPublishers.ofString(update.toString()))
            .header("Content-type", "application/json")
            .version(HttpClient.Version.HTTP_1_1)
            .build();

        _client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }

    public String generateURL() throws UnknownHostException {
        if(_port==-1) {
            return null;
        }
        InetAddress IP = InetAddress.getLocalHost();
        return format("http://localhost:%d", _port);
    }

    public void shutdown() {
    }

    @Override
    public DockerContainerView collect() {
        if (container_id != null) {
            return view.stats(_docker);
        } else return null;
    }

    public static void main(String[] args) throws Exception {
        DUUISimpleMonitor monitor = new DUUISimpleMonitor();
        String containerId = monitor._docker.run("tokenizer:latest", false, false, 9714, false);
        // String containerId2 = monitor._docker.run("sentencizer:latest", false, true, 9714, false);

        // int port = monitor._docker.extract_port_mapping(containerId);
        
        StatsCmd cmd = monitor._docker.getDockerClient().statsCmd(containerId).withNoStream(true);
        AsyncResultCallback<Statistics> statscall = new AsyncResultCallback<>();
        do {
            // TimeUnit.SECONDS.sleep(2);
            cmd.exec(statscall);
            long start = System.currentTimeMillis();
            Statistics stats = statscall.awaitResult();
            System.out.println(stats.getRead());
            // statscall.close();
            System.out.println("DOCKER DRIVER COLLECTION TIME: " + (System.currentTimeMillis() - start));
            // System.out.println(stats);
        } while (true);
        // System.out.println(stats);

        // ContainerState state = monitor._docker.state(containerId);
        // // Thread.sleep(3000); 
        // monitor._docker.start_container(containerId);
        // state = monitor._docker.state(containerId);
        // // Thread.sleep(3000); 
        // monitor._docker.kill_container(containerId);
        // state = monitor._docker.state(containerId);
        // monitor._docker.start_container(containerId);
        // // int port2 = monitor._docker.extract_port_mapping(containerId2);
        
        // // Thread.sleep(3000); 
        // monitor._docker.stop_container(containerId);
        // state = monitor._docker.state(containerId);


    }
 }

 