package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

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
import java.util.ArrayList;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIResource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.DUUIRestClient;

import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;

public class DUUISimpleMonitor implements IDUUIMonitor, IDUUIResource {
    
    // final static HttpClient _client = HttpClient.newBuilder()
    //     .version(HttpClient.Version.HTTP_1_1)
    //     .followRedirects(HttpClient.Redirect.ALWAYS)
    //     .proxy(ProxySelector.getDefault())
    //     .executor(Runnable::run)
    //     .connectTimeout(Duration.ofSeconds(1000)).build();

    final DUUIRestClient _handler = DUUIRestClient.getInstance(); 

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
            _docker.getDockerClient().startContainerCmd(container.getId()).exec();
        }

        System.out.printf("[SimpleMonitor] Monitor dashboard panel is opened at %s\n", generateURL());
        Thread.sleep(2000);
        ResourceManager.getInstance().register(this);
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

        _handler.send(request, HttpResponse.BodyHandlers.ofInputStream());
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

    public static void main(String[] args) throws IOException, InterruptedException {

        DUUISimpleMonitor m = new DUUISimpleMonitor().setup(); 
        org.texttechnologylab.ResourceManager.getInstance().register(m);
        Thread.sleep(60 * 1000);
        
        // JSONObject update = new JSONObject("{'name':'Fuchs', 'title': '0120.xmi', 'scale': 4, 'component': 'Lemma -> Token', 'engine_duration': '00:22:10',  'urlwait': '00:01:41', 'svg': '<svg class=\"mx-auto bg-primary\" height=\"180\" width=\"500\"><polyline points=\"0,40 40,40 40,80 80,80 80,120 120,120 120,160\" style=\"fill:white;stroke:red;stroke-width:4\" /></svg>'}");
        // System.out.println(m.generateURL() + DUUISimpleMonitor.V1_MONITOR_DOCUMENT_UPDATE);
        // HttpRequest request = HttpRequest.newBuilder()
        // .uri(URI.create(m.generateURL() + DUUISimpleMonitor.V1_MONITOR_DOCUMENT_UPDATE))
        // .POST(HttpRequest.BodyPublishers.ofString(update.toString()))
        // .header("Content-type", "application/json")
        // .version(HttpClient.Version.HTTP_1_1)
        // .build();

        // _client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream()).join();
    }

    boolean first = true; 
    @Override
    public JSONObject collect() {

        JSONObject monitorStats = new JSONObject();
        if (container_id != null) {
            
            statsJson.put("container_id", container_id);
            statsJson.put("image_id", _image_id);
            getContainerStats(_docker, statsJson, container_id, _image_id);
            monitorStats.put("monitor", statsJson);
            monitorStats.put("key", "monitor");
            return monitorStats;
        } else return null;
    }

    @Override
    public ResourceManager getResourceManager() {
        return ResourceManager.getInstance();
    }

    @Override
    public void setResourceManager(ResourceManager rm) {
    }

 }

 